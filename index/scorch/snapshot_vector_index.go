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

package scorch

import (
	"context"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

func (is *IndexSnapshot) VectorReader(ctx context.Context, vector []float32,
	field string, k int64) (
	index.VectorReader, error) {

	rv := &IndexSnapshotVectorReader{
		vector:   vector,
		field:    field,
		k:        k,
		snapshot: is,
	}
	rv.currPosting = nil
	rv.currID = rv.currID[:0]

	if rv.postings == nil {
		rv.postings = make([][]segment_api.VecPostingsList, len(is.segment))
	}
	if rv.iterators == nil {
		rv.iterators = make([][]segment_api.VecPostingsIterator, len(is.segment))
	}

	for i, seg := range is.segment {
		if sv, ok := seg.segment.(segment_api.VectorSegment); ok {
			pl, err := sv.SimilarVectors(field, vector, k, seg.deleted)
			if err != nil {
				return nil, err
			}
			rv.postings[0][i] = pl
			rv.iterators[0][i] = pl.Iterator(rv.iterators[0][i])
		}
	}

	return rv, nil
}

func (is *IndexSnapshot) PerSliceVR(ctx context.Context, vector []float32,
	field string, k int64) (index.VectorReader, error) {
	rvs := &IndexSnapshotVectorReader{}
	rvs.segmentOffset = make([]int, len(is.segment))
	rvs.nextSegOffset = 0

	if len(is.slices) == 0 {
		segs := is.SegmentSlices(is.segment)
		is.slices = segs
	}

	rvs.sliceOffset = make([]int, len(is.slices))
	rvs.postings = make([][]segment.VecPostingsList, len(is.slices))
	rvs.iterators = make([][]segment.VecPostingsIterator, len(is.slices))
	rvs.currID = make([]index.IndexInternalID, len(is.slices))
	rvs.currPosting = make([]segment.VecPosting, len(is.slices))
	rvs.totalIterator = make([]segment.VecPostingsIterator, 0)

	var running int

	for i, task := range is.slices {
		rvs.ctx = ctx
		rvs.field = field
		rvs.snapshot = is

		rvs.segmentOffset[i] = 0

		// adding the length of slices of the previous slice
		rvs.sliceOffset[i] = running
		running += len(task)

		if rvs.postings[i] == nil {
			rvs.postings[i] = make([]segment.VecPostingsList, len(task))
		}

		for j, seg := range task {
			if sv, ok := seg.segment.(segment_api.VectorSegment); ok {
				pl, err := sv.SimilarVectors(field, vector, k, seg.deleted)
				if err != nil {
					return nil, err
				}
				rvs.postings[i][j] = pl

				if len(rvs.iterators[i]) == 0 {
					rvs.iterators[i] = make([]segment.VecPostingsIterator, len(task))
				}
				rvs.iterators[i][j] = pl.Iterator(rvs.iterators[i][j])

				rvs.totalIterator = append(rvs.totalIterator, rvs.iterators[i][j])
			}
		}
	}

	return rvs, nil
}
