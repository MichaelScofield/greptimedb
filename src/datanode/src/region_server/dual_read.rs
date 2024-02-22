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

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

use api::v1::region::{QueryRequest, RegionRequestHeader};
use async_stream::try_stream;
use async_trait::async_trait;
use common_meta::datanode_manager::DatanodeManagerRef;
use common_meta::peer::Peer;
use common_query::logical_plan::Expr;
use common_query::physical_plan::DfPhysicalPlanAdapter;
use common_recordbatch::error::DataTypesSnafu;
use common_recordbatch::{
    EmptyRecordBatchStream, RecordBatchStreamWrapper, RecordBatches, SendableRecordBatchStream,
};
use common_telemetry::{debug, warn};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_expr::{logical_plan, Expr as DfExpr, TableProviderFilterPushDown, TableType};
use datatypes::arrow::datatypes::SchemaRef as DfSchemaRef;
use datatypes::data_type::DataType;
use datatypes::prelude::Value;
use futures_util::StreamExt;
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::region_engine::RegionEngineRef;
use store_api::storage::{RegionId, ScanRequest};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::scan::StreamScanAdapter;

use crate::error::{GetRegionMetadataSnafu, Result};
use crate::region_server::TableProviderFactory;

// TODO(LFC): More comments.

pub struct DualReadTableProviderFactory {
    datanode_manager: DatanodeManagerRef,
    pair_region_server: Peer,
}

impl DualReadTableProviderFactory {
    pub fn new(datanode_manager: DatanodeManagerRef, pair_region_server: Peer) -> Self {
        Self {
            datanode_manager,
            pair_region_server,
        }
    }
}

#[async_trait]
impl TableProviderFactory for DualReadTableProviderFactory {
    async fn create(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
    ) -> Result<Arc<dyn TableProvider>> {
        let metadata =
            engine
                .get_metadata(region_id)
                .await
                .with_context(|_| GetRegionMetadataSnafu {
                    engine: engine.name(),
                    region_id,
                })?;
        Ok(Arc::new(DualReadTableProvider::new(
            region_id,
            metadata,
            engine,
            self.datanode_manager.clone(),
            self.pair_region_server.clone(),
        )))
    }
}

struct RowkeyWithFieldProjection {
    rowkey_indices: Vec<usize>,
    field_indices: Vec<usize>,
}

impl RowkeyWithFieldProjection {
    fn new(region_metadata: &RegionMetadata) -> Self {
        let (rowkey_indices, field_indices) = Self::rowkey_field_index_partition(region_metadata);
        Self {
            rowkey_indices,
            field_indices,
        }
    }

    fn rowkey_field_index_partition(region_metadata: &RegionMetadata) -> (Vec<usize>, Vec<usize>) {
        let rowkey_columns = region_metadata
            .primary_key_columns()
            .map(|x| &x.column_schema.name)
            .chain(iter::once(
                &region_metadata.time_index_column().column_schema.name,
            ))
            .collect::<Vec<_>>();

        let mut rowkey_indices = Vec::with_capacity(rowkey_columns.len());
        let mut field_indices =
            Vec::with_capacity(region_metadata.column_metadatas.len() - rowkey_columns.len());

        region_metadata
            .column_metadatas
            .iter()
            .enumerate()
            .for_each(|(i, x)| {
                if rowkey_columns.contains(&&x.column_schema.name) {
                    rowkey_indices.push(i);
                } else {
                    field_indices.push(i);
                }
            });

        (rowkey_indices, field_indices)
    }

    fn scan_projection(&self, user_projection: Option<&Vec<usize>>) -> Vec<usize> {
        let mut scan_projection = self.rowkey_indices.clone();

        if let Some(user_projection) = user_projection {
            scan_projection.extend(
                self.field_indices
                    .iter()
                    .filter(|x| user_projection.contains(x)),
            )
        } else {
            scan_projection.extend(self.field_indices.iter());
        };

        scan_projection
    }
}

struct RowsDeduper<'a> {
    rowkey_with_field_projection: RowkeyWithFieldProjection,
    left_rows: &'a mut dyn Iterator<Item = Vec<Value>>,
    right_rows: &'a mut dyn Iterator<Item = Vec<Value>>,
    curr_left: Option<Vec<Value>>,
    curr_right: Option<Vec<Value>>,
}

impl<'a> RowsDeduper<'a> {
    fn new(
        rowkey_with_field_projection: RowkeyWithFieldProjection,
        left_rows: &'a mut dyn Iterator<Item = Vec<Value>>,
        right_rows: &'a mut dyn Iterator<Item = Vec<Value>>,
    ) -> Self {
        let curr_left = left_rows.next();
        let curr_right = right_rows.next();
        Self {
            rowkey_with_field_projection,
            left_rows,
            right_rows,
            curr_left,
            curr_right,
        }
    }
}

impl<'a> Iterator for RowsDeduper<'a> {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.curr_left.take(), self.curr_right.take()) {
            (Some(x), Some(y)) => {
                let rowkey_len = self.rowkey_with_field_projection.rowkey_indices.len();
                let rowkey_x = &x[..rowkey_len];
                let rowkey_y = &y[..rowkey_len];

                match rowkey_x.cmp(rowkey_y) {
                    Ordering::Less => {
                        self.curr_left = self.left_rows.next();
                        self.curr_right = Some(y);

                        Some(x)
                    }
                    Ordering::Equal => {
                        self.curr_left = self.left_rows.next();
                        self.curr_right = self.right_rows.next();

                        Some(x)
                    }
                    Ordering::Greater => {
                        self.curr_left = Some(x);
                        self.curr_right = self.right_rows.next();

                        Some(y)
                    }
                }
            }
            (Some(x), None) => {
                self.curr_left = self.left_rows.next();

                Some(x)
            }
            (None, Some(y)) => {
                self.curr_right = self.right_rows.next();

                Some(y)
            }
            (None, None) => None,
        }
    }
}

struct DualReadTableProvider {
    region_id: RegionId,
    region_metadata: RegionMetadataRef,
    region_engine: RegionEngineRef,
    datanode_manager: DatanodeManagerRef,
    pair_region_server: Peer,
}

impl DualReadTableProvider {
    fn new(
        region_id: RegionId,
        region_metadata: RegionMetadataRef,
        region_engine: RegionEngineRef,
        datanode_manager: DatanodeManagerRef,
        pair_region_server: Peer,
    ) -> Self {
        Self {
            region_id,
            region_metadata,
            region_engine,
            datanode_manager,
            pair_region_server,
        }
    }

    async fn local_recordbatch_stream(
        &self,
        scan_projection: &[usize],
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<SendableRecordBatchStream> {
        let scan = ScanRequest {
            projection: Some(scan_projection.to_owned()),
            filters: filters.iter().map(|e| Expr::from(e.clone())).collect(),
            limit,
            ..Default::default()
        };

        self.region_engine
            .handle_query(self.region_id, scan)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn remote_recordbatch_stream(
        &self,
        scan_projection: &[usize],
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<SendableRecordBatchStream> {
        let plan = logical_plan::builder::table_scan_with_filters(
            None::<&str>,
            self.schema().as_ref(),
            Some(scan_projection.to_owned()),
            filters.to_vec(),
        )
        .and_then(|x| x.limit(0, limit))
        .and_then(|x| x.build())?;

        let datanode = self
            .datanode_manager
            .datanode(&self.pair_region_server)
            .await;

        let plan = DFLogicalSubstraitConvertor
            .encode(&plan)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .into();

        let request = QueryRequest {
            header: Some(RegionRequestHeader {
                // TODO(LFC): Find another way to prevent dual read loop instead of this temporary "SOURCE = REGION_SERVER" method.
                tracing_context: HashMap::from([(
                    "SOURCE".to_string(),
                    "REGION_SERVER".to_string(),
                )]),
                dbname: "".to_string(),
            }),
            region_id: self.region_id.into(),
            plan,
        };
        datanode
            .handle_query(request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    // TODO(LFC): Make it streaming inside: now for the sake of the simplicity of POC, naively collect all the recordbatches before dedup.
    async fn dedup_recordbatch_streams(
        rowkey_with_field_projection: RowkeyWithFieldProjection,
        local: SendableRecordBatchStream,
        remote: SendableRecordBatchStream,
    ) -> common_recordbatch::error::Result<SendableRecordBatchStream> {
        let local = common_recordbatch::util::collect_batches(local).await?;
        debug!(
            "[DualRead]: Local RecordBatches:\n{}",
            local.pretty_print()?
        );

        let remote = common_recordbatch::util::collect_batches(remote).await?;
        debug!(
            "[DualRead]: Remote RecordBatches:\n{}",
            remote.pretty_print()?
        );

        assert_eq!(local.schema(), remote.schema());
        let schema = local.schema();

        let mut local_rows = local.iter().flat_map(|x| x.rows());
        let mut remote_rows = remote.iter().flat_map(|x| x.rows());

        let deduper = RowsDeduper::new(
            rowkey_with_field_projection,
            &mut local_rows,
            &mut remote_rows,
        );
        let dedup_rows = deduper.into_iter().collect::<Vec<_>>();

        let Some(first_row) = dedup_rows.first() else {
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema)));
        };

        let mut columns = Vec::with_capacity(first_row.len());

        // Allowed this clippy warning to make the column-row iteration codes look more clear.
        #[allow(clippy::needless_range_loop)]
        for j in 0..first_row.len() {
            let mut vector_builder = first_row[j]
                .data_type()
                .create_mutable_vector(dedup_rows.len());
            for i in 0..dedup_rows.len() {
                vector_builder.push_value_ref(dedup_rows[i][j].as_value_ref());
            }
            columns.push(vector_builder.to_vector());
        }

        let recordbatch = RecordBatches::try_from_columns(schema, columns)?;
        debug!(
            "[DualRead]: Dedup Recordbatches:\n{}",
            recordbatch.pretty_print()?
        );

        Ok(recordbatch.as_stream())
    }

    async fn project_recordbatch_stream(
        &self,
        projection: Option<Vec<usize>>,
        mut stream: SendableRecordBatchStream,
    ) -> common_recordbatch::error::Result<SendableRecordBatchStream> {
        let projected_schema = if let Some(projection) = &projection {
            let projected_schema = self
                .region_metadata
                .schema
                .try_project(projection)
                .context(DataTypesSnafu)?;
            Arc::new(projected_schema)
        } else {
            self.region_metadata.schema.clone()
        };

        let projected_stream = try_stream! {
            while let Some(x) = stream.next().await {
                let recordbatch = x?;

                let projected_recordbatch = if let Some(projection) = &projection {
                    recordbatch.try_project(projection)?
                } else {
                    recordbatch
                };
                debug!("[DualRead]: yield projected Recordbatch:\n{}", projected_recordbatch.pretty_print());

                yield projected_recordbatch
            }
        };

        Ok(Box::pin(RecordBatchStreamWrapper::new(
            projected_schema,
            Box::pin(projected_stream),
        )))
    }
}

#[async_trait]
impl TableProvider for DualReadTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.region_metadata.schema.arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let rowkey_with_field_projection = RowkeyWithFieldProjection::new(&self.region_metadata);
        let scan_projection = rowkey_with_field_projection.scan_projection(projection);

        let local = self
            .local_recordbatch_stream(&scan_projection, filters, limit)
            .await?;

        let stream = match self
            .remote_recordbatch_stream(&scan_projection, filters, limit)
            .await
        {
            Ok(remote) => {
                Self::dedup_recordbatch_streams(rowkey_with_field_projection, local, remote)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            }
            Err(e) => {
                warn!(e; "Failed to scan pair region server, use local result directly!");
                local
            }
        };

        let projected = self
            .project_recordbatch_stream(projection.cloned(), stream)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(DfPhysicalPlanAdapter(Arc::new(
            StreamScanAdapter::new(projected),
        ))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
