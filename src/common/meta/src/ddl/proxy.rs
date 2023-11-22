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

use api::v1::TableId as PbTableId;
use async_trait::async_trait;
use common_telemetry::error;
use snafu::OptionExt;
use table::metadata::TableId;

use crate::ddl::{DdlTaskExecutor, DdlTaskExecutorRef, ExecutorContext};
use crate::error::{Result, UnexpectedSnafu};
use crate::rpc::ddl::{DdlTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse};

pub struct ProxyDdlTaskExecutor {
    executors: Vec<DdlTaskExecutorRef>,
}

impl ProxyDdlTaskExecutor {
    pub fn new(executors: Vec<DdlTaskExecutorRef>) -> Self {
        ProxyDdlTaskExecutor { executors }
    }

    fn fill_table_id(table_id: Option<TableId>, request: &mut SubmitDdlTaskRequest) {
        let Some(table_id) = table_id else { return };
        if let DdlTask::CreateTable(x) = &mut request.task {
            let _ = x.create_table.table_id.insert(PbTableId { id: table_id });
        }
    }
}

#[async_trait]
impl DdlTaskExecutor for ProxyDdlTaskExecutor {
    async fn submit_ddl_task(
        &self,
        ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        let (first, rests) = self.executors.split_first().context(UnexpectedSnafu {
            err_msg: "ProxyDdlTaskExecutor has no executors!",
        })?;

        let response = first.submit_ddl_task(ctx, request.clone()).await?;

        for rest in rests {
            let mut request = request.clone();
            Self::fill_table_id(response.table_id, &mut request);

            if let Err(e) = rest.submit_ddl_task(ctx, request).await {
                error!(e; "Failed to submit DDL task!");
            }
        }

        Ok(response)
    }
}
