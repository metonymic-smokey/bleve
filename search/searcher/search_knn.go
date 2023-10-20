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

package searcher

import (
	"context"
	"fmt"
	"log"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	index "github.com/blevesearch/bleve_index_api"
)

type KNNSearcher struct {
	field        string
	vector       []float32
	k            int64
	indexReader  index.IndexReader
	vectorReader index.VectorReader
	scorer       *scorer.KNNQueryScorer
	count        uint64
	vd           index.VectorDoc
}

func NewKNNSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions, field string, vector []float32, k int64,
	boost float64, similarityMetric string) (search.Searcher, error) {
	if vr, ok := i.(index.VectorIndexReader); ok {
		vectorReader, err := vr.PerSliceVR(ctx, vector, field, k)
		if err != nil {
			return nil, err
		}

		knnScorer := scorer.NewKNNQueryScorer(vector, field, boost,
			options, similarityMetric)
		return &KNNSearcher{
			indexReader:  i,
			vectorReader: vectorReader,
			field:        field,
			vector:       vector,
			k:            k,
			scorer:       knnScorer,
		}, nil
	}
	return nil, nil
}

func (s *KNNSearcher) NumSlices() int {
	// Adding this check in case the readers in the searcher haven't implemented
	// the interface.
	if x, ok := s.vectorReader.(index.ConcurrentVectorReader); ok {
		return x.NumSlices()
	}
	return 1
}

func (s *KNNSearcher) NextInSlice(sliceIdx int, ctx *search.SearchContext) (*search.DocumentMatch, error) {
	var err error
	var vectorMatch *index.VectorDoc
	if x, ok := s.vectorReader.(index.ConcurrentVectorReader); ok {
		vectorMatch, err = x.NextInSlice(sliceIdx, &index.VectorDoc{})
		if err != nil {
			return nil, err
		}
	} else {
		vectorMatch, err = s.vectorReader.Next(&index.VectorDoc{}) // default fallback option
		if err != nil {
			return nil, err
		}
	}

	if vectorMatch == nil {
		return nil, nil
	}

	// score match
	docMatch := s.scorer.Score(ctx, vectorMatch)
	fmt.Printf("vector match is %+v \n", vectorMatch)
	// return doc match
	return docMatch, nil
}

func (s *KNNSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (
	*search.DocumentMatch, error) {
	knnMatch, err := s.vectorReader.Next(s.vd.Reset())
	if err != nil {
		return nil, err
	}

	if knnMatch == nil {
		return nil, nil
	}

	docMatch := s.scorer.Score(ctx, knnMatch)

	return docMatch, nil
}

func (s *KNNSearcher) Close() error {
	return s.vectorReader.Close()
}

func (s *KNNSearcher) Count() uint64 {
	return s.vectorReader.Count()
}

func (s *KNNSearcher) DocumentMatchPoolSize() int {
	return 1
}

func (s *KNNSearcher) Min() int {
	return 0
}

func (s *KNNSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	knnMatch, err := s.vectorReader.Next(s.vd.Reset())
	if err != nil {
		return nil, err
	}

	if knnMatch == nil {
		log.Printf("knnMatch is nil...")
		return nil, nil
	}

	docMatch := s.scorer.Score(ctx, knnMatch)
	log.Printf("knnMatch is %+v", knnMatch)

	return docMatch, nil
}

func (s *KNNSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *KNNSearcher) Size() int {
	return 0
}

func (s *KNNSearcher) Weight() float64 {
	return s.scorer.Weight()
}
