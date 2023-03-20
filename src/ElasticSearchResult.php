<?php
namespace ArmoniaElasticSearch;

class ElasticSearchResult
{
    public function newSearchResult(
        array $rawResult,
        int $page = 0,
        int $pageSize = 10,
        string $highlightFields = "",
        string $q = ""
    ) :array {
        $args = [];

        $args['page']      = $page;
        $args['page_size'] = $pageSize;

        if (!empty($rawResult['error'])) {
            $args['error']   = $rawResult['error'];
        } else {
            $args['timetaken']             = $rawResult['took'];
            $args['total_results']         = $rawResult['hits']['total']['value'];
            $args['total_result_relation'] = $rawResult['hits']['total']['relation'];
            $args['results']               = $this->_hitsToResults($rawResult['hits']['hits'], $highlightFields);
            
            if (!empty($rawResult['facets'])) {
                $args['facets'] = $this->_transformFacets($rawResult['facets']);
            }

            if (!empty($rawResult['aggregations'])) {
                $args['aggregations'] = $rawResult['aggregations'];
            }

            if (!empty($rawResult['suggest'])) {
                $args['suggestions'] = $rawResult['suggest'];
            }
        }

        return $args;
    }

    // PRIVATE HELPERS
    private function _hitsToResults(
        array $hits,
        string $highlightFields
    ) :array {
        $highlights = explode(",", $highlightFields);
        $results    = [];
        $data       = [];

        foreach ($hits as $hitIndex => $hit) {
            $data = $hit['_source'];

            $data['type']  = $hit['_type'] ?? '';
            $data['score'] = (!empty($hit['_score']))? $hit['_score'] : 0;

            foreach ($highlights as $highlight) {
                $data[$highlight."_highlight"] = (!empty($hit['highlight']))? $this->_formatHighlight($hit['highlight'], $highlight) : "";
            }
            
            $results[] = $data;
        }

        return $results;
    }

    private function _formatHighlight(
        array $highlights,
        string $highlightField
    ) :string {
        $highlight      = [];
        $highLightArray = [];
        $maxHighlights  = 3;
        $dotdotdot      = " &#133; ";

        if (!empty($highlights[$highlightField])) {
            $highlightArray = $highlights[$highlightField];

            foreach ($highlightArray as $highlightArrayIndex => $highlightArrayDetail) {
                if ($highlightArrayIndex < $maxHighlights) {
                    $highlight[] = $highlightArrayDetail;
                }
            }
        }

        return implode($dotdotdot, $highlight);
    }

    private function _transformFacets(
        array $facets
    ) :array {
        $transformed = [];
        $facetKeys   = array_keys($facets);

        foreach ($facetKeys as $facetKey) {
            $transformed[$facetKey] = [];

            foreach ($facets[$facetKey]['terms'] as $termDetail) {
                $transformed[$facetKey][] = [
                    'id'    => $termDetail['term'],
                    'label' => $termDetail['term'],
                    'count' => $termDetail['count']
                ];
            }
        }

        return $transformed;
    }
}
