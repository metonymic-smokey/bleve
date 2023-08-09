//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !densevector
// +build !densevector

package bleve

import (
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

// A SearchRequest describes all the parameters
// needed to search the index.
// Query is required.
// Size/From describe how much and which part of the
// result set to return.
// Highlight describes optional search result
// highlighting.
// Fields describes a list of field values which
// should be retrieved for result documents, provided they
// were stored while indexing.
// Facets describe the set of facets to be computed.
// Explain triggers inclusion of additional search
// result score explanations.
// Sort describes the desired order for the results to be returned.
// Score controls the kind of scoring performed
// SearchAfter supports deep paging by providing a minimum sort key
// SearchBefore supports deep paging by providing a maximum sort key
// sortFunc specifies the sort implementation to use for sorting results.
//
// A special field named "*" can be used to return all fields.
type SearchRequest struct {
	Query            query.Query       `json:"query"`
	Size             int               `json:"size"`
	From             int               `json:"from"`
	Highlight        *HighlightRequest `json:"highlight"`
	Fields           []string          `json:"fields"`
	Facets           FacetsRequest     `json:"facets"`
	Explain          bool              `json:"explain"`
	Sort             search.SortOrder  `json:"sort"`
	IncludeLocations bool              `json:"includeLocations"`
	Score            string            `json:"score,omitempty"`
	SearchAfter      []string          `json:"search_after"`
	SearchBefore     []string          `json:"search_before"`

	sortFunc func(sort.Interface)
}
