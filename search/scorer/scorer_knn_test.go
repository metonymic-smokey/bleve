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

//go:build vectors
// +build vectors

package scorer

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

func TestKNNScorerExplanation(t *testing.T) {
	var queryVector []float32
	// arbitrary vector of dims: 64
	for i := 0; i < 64; i++ {
		queryVector = append(queryVector, float32(i))
	}

	var resVector []float32
	// arbitrary res vector.
	for i := 0; i < 64; i++ {
		resVector = append(resVector, float32(i))
	}

	tests := []struct {
		termMatch *index.VectorDoc
		scorer    *KNNQueryScorer
		norm      float64
		result    *search.DocumentMatch
	}{
		{
			termMatch: &index.VectorDoc{
				ID:     index.IndexInternalID("one"),
				Score:  0.5,
				Vector: resVector,
			},
			norm: 1.0,
			scorer: NewKNNQueryScorer(queryVector, "desc", 1.0,
				search.SearcherOptions{Explain: true}, util.EuclideanDistance),
			// Specifically testing EuclideanDistance since that involves score inversion.
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           0.5,
				Expl: &search.Explanation{
					Value:   1 / 0.5,
					Message: "fieldWeight(desc in doc one), score of:",
					Children: []*search.Explanation{
						{Value: 1 / 0.5,
							Message: "vector(field(desc:one) with similarity_metric(l2_norm)=2.000000",
						},
					},
				},
			},
		},
		{
			termMatch: &index.VectorDoc{
				ID:     index.IndexInternalID("one"),
				Score:  0.5,
				Vector: resVector,
			},
			norm: 1.0,
			scorer: NewKNNQueryScorer(queryVector, "desc", 1.0,
				search.SearcherOptions{Explain: true}, util.CosineSimilarity),
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           0.5,
				Expl: &search.Explanation{
					Value:   0.5,
					Message: "fieldWeight(desc in doc one), score of:",
					Children: []*search.Explanation{
						{Value: 0.5,
							Message: "vector(field(desc:one) with similarity_metric(dot_product)=0.500000",
						},
					},
				},
			},
		},
		{
			termMatch: &index.VectorDoc{
				ID:     index.IndexInternalID("one"),
				Score:  0.5,
				Vector: resVector,
			},
			norm: 0.5,
			scorer: NewKNNQueryScorer(queryVector, "desc", 1.0,
				search.SearcherOptions{Explain: true}, util.CosineSimilarity),
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           0.25,
				Expl: &search.Explanation{
					Value:   0.25,
					Message: "weight(desc:[0.000000 1.000000 2.000000 3.000000 4.000000 5.000000 6.000000 7.000000 8.000000 9.000000 10.000000 11.000000 12.000000 13.000000 14.000000 15.000000 16.000000 17.000000 18.000000 19.000000 20.000000 21.000000 22.000000 23.000000 24.000000 25.000000 26.000000 27.000000 28.000000 29.000000 30.000000 31.000000 32.000000 33.000000 34.000000 35.000000 36.000000 37.000000 38.000000 39.000000 40.000000 41.000000 42.000000 43.000000 44.000000 45.000000 46.000000 47.000000 48.000000 49.000000 50.000000 51.000000 52.000000 53.000000 54.000000 55.000000 56.000000 57.000000 58.000000 59.000000 60.000000 61.000000 62.000000 63.000000]^1.000000 in ), product of:",
					Children: []*search.Explanation{
						{
							Value:   0.25,
							Message: "queryWeight(desc:[0.000000 1.000000 2.000000 3.000000 4.000000 5.000000 6.000000 7.000000 8.000000 9.000000 10.000000 11.000000 12.000000 13.000000 14.000000 15.000000 16.000000 17.000000 18.000000 19.000000 20.000000 21.000000 22.000000 23.000000 24.000000 25.000000 26.000000 27.000000 28.000000 29.000000 30.000000 31.000000 32.000000 33.000000 34.000000 35.000000 36.000000 37.000000 38.000000 39.000000 40.000000 41.000000 42.000000 43.000000 44.000000 45.000000 46.000000 47.000000 48.000000 49.000000 50.000000 51.000000 52.000000 53.000000 54.000000 55.000000 56.000000 57.000000 58.000000 59.000000 60.000000 61.000000 62.000000 63.000000]^1.000000), product of:",
							Children: []*search.Explanation{
								{
									Value:   1,
									Message: "boost",
								},
								{
									Value:   0.5,
									Message: "queryNorm",
								},
							},
						},
						{
							Value:   0.5,
							Message: "fieldWeight(desc in doc one), product of:",
							Children: []*search.Explanation{
								{
									Value:   0.5,
									Message: "vector(field(desc:one) with similarity_metric(dot_product)=0.500000",
								},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		ctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(1, 0),
		}
		test.scorer.SetQueryNorm(test.norm)
		actual := test.scorer.Score(ctx, test.termMatch)
		actual.Complete(nil)

		if !reflect.DeepEqual(actual.Expl, test.result.Expl) {
			t.Errorf("expected %#v got %#v for %#v", test.result.Expl,
				actual.Expl, test.termMatch)
		}
	}
}
