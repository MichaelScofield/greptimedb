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

use crate::error::{Error, Result, UnsupportedIndexTypeSnafu};

const INDEX_TIMESTAMP: &str = "timestamp";
const INDEX_TIMEINDEX: &str = "time";
const INDEX_TAG: &str = "tag";
const INDEX_FULLTEXT: &str = "fulltext";
const INDEX_SKIPPING: &str = "skipping";
const INDEX_INVERTED: &str = "inverted";

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(clippy::enum_variant_names)]
pub enum Index {
    Time,
    // deprecated, use Inverted instead
    Tag,
    Fulltext,
    Skipping,
    Inverted,
}

impl std::fmt::Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let index = match self {
            Index::Time => INDEX_TIMEINDEX,
            Index::Tag => INDEX_TAG,
            Index::Fulltext => INDEX_FULLTEXT,
            Index::Skipping => INDEX_SKIPPING,
            Index::Inverted => INDEX_INVERTED,
        };

        write!(f, "{}", index)
    }
}

impl TryFrom<String> for Index {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Index::try_from(value.as_str())
    }
}

impl TryFrom<&str> for Index {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            INDEX_TIMESTAMP | INDEX_TIMEINDEX => Ok(Index::Time),
            INDEX_TAG => Ok(Index::Tag),
            INDEX_FULLTEXT => Ok(Index::Fulltext),
            INDEX_SKIPPING => Ok(Index::Skipping),
            INDEX_INVERTED => Ok(Index::Inverted),
            _ => UnsupportedIndexTypeSnafu { value }.fail(),
        }
    }
}
