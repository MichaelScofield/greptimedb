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

mod cli;
mod datanode;
mod flownode;
mod frontend;
mod meta_srv;
mod options;

pub use cli::SubCommand;
pub use datanode::{setup_datanode_plugins, start_datanode_plugins};
pub use flownode::{setup_flownode_plugins, start_flownode_plugins};
pub use frontend::{setup_frontend_plugins, start_frontend_plugins};
pub use meta_srv::{setup_metasrv_plugins, start_metasrv_plugins};
pub use options::PluginOptions;
