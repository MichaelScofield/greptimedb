// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api::v1::region::{
    region_request, QueryRequest, RegionRequest, RegionRequestHeader, RegionResponse,
};
use async_trait::async_trait;
use client::client_manager::DatanodeClients;
use client::region::check_response_header;
use common_error::ext::BoxedError;
use common_meta::datanode_manager::{
    AffectedRows, Datanode, DatanodeManager, DatanodeManagerRef, DatanodeRef,
};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::peer::Peer;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{tracing, warn};
use datanode::region_server::RegionServer;
use servers::grpc::region_server::RegionServerHandler;
use snafu::{OptionExt, ResultExt};

use crate::error::{InvalidRegionRequestSnafu, InvokeRegionServerSnafu, Result};

pub struct StandaloneDatanodeManager {
    region_server: RegionServer,

    requester: Option<PairRegionServerRequester>,
}

impl StandaloneDatanodeManager {
    pub fn new(region_server: RegionServer, requester: Option<PairRegionServerRequester>) -> Self {
        Self {
            region_server,
            requester,
        }
    }
}

#[async_trait]
impl DatanodeManager for StandaloneDatanodeManager {
    async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
        RegionInvoker::arc(self.region_server.clone(), self.requester.clone())
    }
}

#[derive(Clone)]
pub struct PairRegionServerRequester {
    datanode_manager: DatanodeManagerRef,
    peer: Peer,

    // TODO(LFC): Find a (much) better way to do this kind of relaxed logging.
    last_failed_log_time: Arc<Mutex<Option<Instant>>>,
    failed_log_count: Arc<AtomicU32>,
}

impl PairRegionServerRequester {
    pub fn new(peer: Peer) -> Self {
        Self {
            datanode_manager: Arc::new(DatanodeClients::default()),
            peer,
            last_failed_log_time: Arc::new(Mutex::new(None)),
            failed_log_count: Arc::new(AtomicU32::new(0)),
        }
    }

    async fn do_request(&self, request: RegionRequest) {
        let pair_datanode = self.datanode_manager.datanode(&self.peer).await;

        if let Err(e) = pair_datanode.handle(request).await {
            let mut t = self.last_failed_log_time.lock().unwrap();

            let t = t.get_or_insert_with(|| Instant::now() - Duration::from_secs(4));

            if t.elapsed() > Duration::from_secs(3) {
                *t = Instant::now();

                let c = self.failed_log_count.swap(0, Ordering::Relaxed);
                if c > 1 {
                    warn!(e; "Failed to request pair RegionServer: {}, total count: {}", &self.peer, c);
                } else {
                    warn!(e; "Failed to request pair RegionServer: {}", &self.peer);
                }
            } else {
                self.failed_log_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Relative to [client::region::RegionRequester]
pub struct RegionInvoker {
    region_server: RegionServer,

    requester: Option<PairRegionServerRequester>,
}

impl RegionInvoker {
    pub fn arc(
        region_server: RegionServer,
        requester: Option<PairRegionServerRequester>,
    ) -> Arc<Self> {
        Arc::new(Self {
            region_server,
            requester,
        })
    }

    async fn handle_inner(&self, request: RegionRequest) -> Result<RegionResponse> {
        let body = request.body.with_context(|| InvalidRegionRequestSnafu {
            reason: "body not found",
        })?;

        if let Some(requester) = &self.requester {
            if matches!(
                body,
                region_request::Body::Inserts(_) | region_request::Body::Deletes(_)
            ) {
                let request = RegionRequest {
                    header: Some(RegionRequestHeader::default()),
                    body: Some(body.clone()),
                };

                let requester = requester.clone();
                common_runtime::spawn_write(async move { requester.do_request(request).await });
            }
        }

        self.region_server
            .handle(body)
            .await
            .context(InvokeRegionServerSnafu)
    }
}

#[async_trait]
impl Datanode for RegionInvoker {
    async fn handle(&self, request: RegionRequest) -> MetaResult<AffectedRows> {
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("RegionInvoker::handle_region_request"));
        let response = self
            .handle_inner(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        check_response_header(response.header)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        Ok(response.affected_rows)
    }

    async fn handle_query(&self, request: QueryRequest) -> MetaResult<SendableRecordBatchStream> {
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("RegionInvoker::handle_query"));
        self.region_server
            .handle_read(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}
