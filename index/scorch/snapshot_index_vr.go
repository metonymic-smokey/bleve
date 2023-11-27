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
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

var reflectStaticSizeIndexSnapshotVectorReader int

func init() {
	var istfr IndexSnapshotVectorReader
	reflectStaticSizeIndexSnapshotVectorReader = int(reflect.TypeOf(istfr).Size())
}

type IndexSnapshotVectorReader struct {
	vector        []float32
	field         string
	k             int64
	snapshot      *IndexSnapshot
	postings      [][]segment_api.VecPostingsList
	iterators     [][]segment_api.VecPostingsIterator
	segmentOffset []int
	currPosting   []segment_api.VecPosting
	currID        []index.IndexInternalID
	ctx           context.Context
	bytesRead     uint64

	// slice related fields
	// not reusing existing ones for now - backward compatib?
	sliceOffset []int

	nextSegOffset   int
	nextSliceOffset int
	totalIterator   []segment_api.VecPostingsIterator
}

func (i *IndexSnapshotVectorReader) Segments() int {
	return len(i.snapshot.Segments())
}

func (i *IndexSnapshotVectorReader) Size() int {
	sizeInBytes := reflectStaticSizeIndexSnapshotVectorReader + size.SizeOfPtr +
		len(i.vector) + len(i.field) + len(i.currID)

	for _, slicePosting := range i.postings {
		for _, entry := range slicePosting {
			sizeInBytes += entry.Size()
		}
	}

	for _, sliceItr := range i.iterators {
		for _, entry := range sliceItr {
			sizeInBytes += entry.Size()
		}
	}

	for _, sliceCurrPosting := range i.currPosting {
		if sliceCurrPosting != nil {
			sizeInBytes += sliceCurrPosting.Size()
		}
	}

	return sizeInBytes
}

func (i *IndexSnapshotVectorReader) Next(preAlloced *index.VectorDoc) (
	*index.VectorDoc, error) {
	rv := preAlloced
	if rv == nil {
		rv = &index.VectorDoc{}
	}

	for i.nextSegOffset < len(i.totalIterator) {
		prevBytesRead := i.totalIterator[i.nextSegOffset].BytesRead()
		next, err := i.totalIterator[i.nextSegOffset].Next()
		if err != nil {
			return nil, err
		}
		if next != nil {
			// make segment number into global number by adding offset
			globalOffset := i.snapshot.offsets[i.nextSegOffset]
			nnum := next.Number()
			rv.ID = docNumberToBytes(rv.ID, nnum+globalOffset)
			rv.Score = float64(next.Score())

			i.currID[0] = rv.ID
			i.currPosting[0] = next

			bytesRead := i.totalIterator[i.nextSegOffset].BytesRead()
			if bytesRead > prevBytesRead {
				i.incrementBytesRead(bytesRead - prevBytesRead)
			}

			return rv, nil
		}
		i.nextSegOffset++
	}

	return nil, nil
}

func (i *IndexSnapshotVectorReader) incrementBytesRead(val uint64) {
	i.bytesRead += val
}

func (i *IndexSnapshotVectorReader) NumSlices() int {
	return len(i.snapshot.slices)
}

func (i *IndexSnapshotVectorReader) NextInSlice(sliceIndex int, preAlloced *index.VectorDoc) (*index.VectorDoc, error) {
	rv := &index.VectorDoc{}
	// find the next hit
	for i.segmentOffset[sliceIndex] < len(i.iterators[sliceIndex]) {
		next, err := i.iterators[sliceIndex][i.segmentOffset[sliceIndex]].Next()
		if err != nil {
			return nil, err
		}
		if next != nil {
			// make segment number into global number by adding offset
			// globalSegOffset --> global offset of seg number
			globalSegOffset := i.segmentOffset[sliceIndex] + i.sliceOffset[sliceIndex]
			globalOffset := i.snapshot.offsets[globalSegOffset]
			nnum := next.Number()
			rv.ID = docNumberToBytes(rv.ID, nnum+globalOffset)

			i.currID[sliceIndex] = rv.ID
			i.currPosting[sliceIndex] = next

			return rv, nil
		}
		i.segmentOffset[sliceIndex]++
	}
	return nil, nil
}

func (i *IndexSnapshotVectorReader) Advance(ID index.IndexInternalID,
	preAlloced *index.VectorDoc) (*index.VectorDoc, error) {

	// Find out which slice the segment is in --> then for that slice,
	// update the segment offset.
	// So both Next() and NextInSlice() calls will work seamlessly.
	slice := i.snapshot.sliceForASegOffset(i.nextSegOffset)

	if i.currPosting != nil && bytes.Compare(i.currID[slice], ID) >= 0 {
		i2, err := i.snapshot.PerSliceVR(i.ctx, i.vector, i.field, i.k)
		if err != nil {
			return nil, err
		}
		// close the current term field reader before replacing it with a new one
		_ = i.Close()
		*i = *(i2.(*IndexSnapshotVectorReader))
	}

	num, err := docInternalToNumber(ID)
	if err != nil {
		return nil, fmt.Errorf("error converting to doc number % x - %v", ID, err)
	}
	segIndex, ldocNum := i.snapshot.segmentIndexAndLocalDocNumFromGlobal(num)
	if segIndex >= len(i.snapshot.segment) {
		return nil, fmt.Errorf("computed segment index %d out of bounds %d",
			segIndex, len(i.snapshot.segment))
	}
	// Need to advance the nextSegOffset to the right segment
	// so for subsequent Next() calls, will skip directly to this segment.
	i.nextSegOffset = segIndex

	next, err := i.totalIterator[segIndex].Advance(ldocNum)
	if err != nil {
		return nil, err
	}
	if next == nil {
		// we jumped directly to the segment that should have contained it
		// but it wasn't there, so reuse Next() which should correctly
		// get the next hit after it (we moved i.segmentOffset)
		return i.Next(preAlloced)
	}

	if preAlloced == nil {
		preAlloced = &index.VectorDoc{}
	}

	preAlloced.ID = docNumberToBytes(preAlloced.ID, next.Number()+
		i.snapshot.offsets[segIndex])
	i.currID[slice] = preAlloced.ID
	i.currPosting[slice] = next

	return preAlloced, nil
}

func (i *IndexSnapshotVectorReader) Count() uint64 {
	var rv uint64
	for _, slicePosting := range i.postings {
		for _, posting := range slicePosting {
			rv += posting.Count()
		}
	}
	return rv
}

func (i *IndexSnapshotVectorReader) Close() error {
	// TODO Consider if any scope of recycling here.
	return nil
}
