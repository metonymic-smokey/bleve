package searcher

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
)

func TestPointContains(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: []float64{1.0, 1.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point contains itself",
			QueryType:        "contains",
		},
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: []float64{1.0, 1.1},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point does not contain a different point",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPointLinestringContains(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "point does not cover different linestring",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPointPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "point does not cover polygon",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinestringPointContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		// check these cases
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{1.0, 1.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point at start of linestring",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{2.0, 2.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point in the middle of linestring",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{3.0, 3.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point at end of linestring",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{1.5, 1.50017},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point in between linestring",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{4, 5},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point not contained by linestring",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
