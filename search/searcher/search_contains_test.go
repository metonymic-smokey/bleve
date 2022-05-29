package searcher

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
)

var (
	leftRectEdgeMultiPoint [][]float64   = [][]float64{{-1, 0.2}, {-0.9, 0.1}}
	leftRectWithHole       [][][]float64 = [][][]float64{{{-1, 0}, {0, 0}, {0, 1}, {-1, 1}, {-1, 0}},
		{{-0.75, 0.25}, {-0.75, -0.75}, {-0.25, 0.75}, {-0.25, 0.25}, {-0.74, 0.25}}}
	leftRectEdgePoint  []float64   = []float64{-1, 0.2}
	leftRectMultiPoint [][]float64 = [][]float64{{0.5, 0.5}, {-0.9, 0.1}}
)

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
			QueryShape:       rightRectPoint,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "point inside polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRectPoint,
			DocShapeVertices: nil,
			DocShapeName:     "",
			Expected:         nil,
			Desc:             "empty polygon",
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

func TestPolygonPointContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       rightRect,
			DocShapeVertices: rightRectPoint,
			DocShapeName:     "point1",
			Expected:         nil, // nil since point is a non-closed shape
			Desc:             "point inside polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRect,
			DocShapeVertices: leftRectEdgePoint,
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point on edge of polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRectWithHole,
			DocShapeVertices: leftRectPoint,
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point in polygon's hole",
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
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
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

func TestMultiPointPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRectEdgeMultiPoint,
			DocShapeVertices: leftRectWithHole,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "multi point inside polygon with hole",
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
				true, indexReader, test.QueryShape, "geometry")
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

func TestPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRect,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "polygons sharing an edge",
			QueryType:        "contains",
		},
		{
			QueryShape:       rightRect,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "coincident polygons",
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
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
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

func TestPolygonMultiPointContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRect,
			DocShapeVertices: leftRectEdgeMultiPoint,
			DocShapeName:     "multipoint1",
			Expected:         nil, // nil since multipoint is a non-closed shape
			Desc:             "multiple points on polygon edge",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRect,
			DocShapeVertices: leftRectMultiPoint,
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multiple points, both outside and inside polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRectWithHole,
			DocShapeVertices: leftRectMultiPoint,
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multiple points in polygon hole",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
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
