//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build densevector
// +build densevector

package bleve

import "github.com/blevesearch/bleve/v2/search/query"

func addSimilarityQuery(req *SearchRequest) query.Query {
	if req.Similarity != nil {
		similarityQuery := query.NewSimilarityQuery(req.Similarity.Vector)
		similarityQuery.SetFieldVal(req.Similarity.Field)
		similarityQuery.SetK(req.Similarity.K)
		return query.NewConjunctionQuery([]query.Query{req.Query, similarityQuery})
	}
	return nil
}
