<?php

namespace ArmoniaElasticSearch\Test;

use ArmoniaElasticSearch\ElasticSearchEngine;
use ArmoniaElasticSearch\ElasticSearchResult;

use PHPUnit\Framework\TestCase;

class ElasticSearchEngineTest extends TestCase {

    private $_engine;

    public function setUp(): void {
        $this->assertNotEmpty(getenv('UNIT_TEST_ELASTIC_SEARCH_HOST'), "Please set the env UNIT_TEST_ELASTIC_SEARCH_HOST");
        // Settings here

        $this->_engine = new ElasticSearchEngine([
            'hosts' => [getenv('UNIT_TEST_ELASTIC_SEARCH_HOST')],
            'retry' => 2
        ]);

    }

    private function getIndexName() {
        $this->assertNotEmpty(getenv('UNIT_TEST_ELASTIC_SEARCH_INDEX'), "Please set the env UNIT_TEST_ELASTIC_SEARCH_INDEX");
        return getenv('UNIT_TEST_ELASTIC_SEARCH_INDEX');
    }

    public function testSearchSingle() {
        $query = [];
        $query['bool']['filter'][] = [
            'terms' => [
                'id' => [10000001]
            ]
        ];

        $result = $this->_engine->search(
                $this->getIndexName(),
                [],
                $query
            );

        $result_array = $result->asArray();

        $this->assertEquals(1, $result_array['hits']['total']['value']);
        $this->assertEquals(10000001, $result_array['hits']['hits'][0]['_source']['id']);

        $this->assertArrayHasKey('order_number', $result_array['hits']['hits'][0]['_source']);
        $this->assertArrayHasKey('order_date', $result_array['hits']['hits'][0]['_source']);
    }


    public function testSearchNoCondition() {
        $query = [];
        $result = $this->_engine->search(
            $this->getIndexName(),
            ['id', 'order_number'],
            $query
        );

        $result_array = $result->asArray();

        $this->assertGreaterThan(1, $result_array['hits']['total']['value']);
        $this->assertIsInt($result_array['hits']['hits'][0]['_source']['id']);

        $this->assertArrayHasKey('order_number', $result_array['hits']['hits'][0]['_source']);
        $this->assertArrayNotHasKey('order_date', $result_array['hits']['hits'][0]['_source']);
    }


    public function testSearchAllStandardParam() {
        $query = [];
        $query['bool']['filter'][] = [
            'terms' => [
                'marketplace_id' => [1]
            ]
        ];

        $result = $this->_engine->search(
            $this->getIndexName(),
            ['order_number'],
            $query,
            10,
            5,
            ['id']
        );

        $result_array = $result->asArray();

        $this->assertGreaterThan(1, $result_array['hits']['total']['value']);

        $this->assertArrayHasKey('order_number', $result_array['hits']['hits'][0]['_source']);
        $this->assertArrayNotHasKey('order_date', $result_array['hits']['hits'][0]['_source']);
    }

    public function testSearchAggs() {
        $query = [];
        $aggs = [
            "test_aggs" => [
              "terms" => [
                  "field" => "marketplace_id"
              ]
            ]
        ];


        $result = $this->_engine->search(
            $this->getIndexName(),
            ['order_number'],
            $query,
            10,
            5,
            ['id'],
            $aggs
        );

        $result_array = $result->asArray();

        $this->assertGreaterThan(1, $result_array['hits']['total']['value']);

        $this->assertArrayHasKey('order_number', $result_array['hits']['hits'][0]['_source']);
        $this->assertArrayNotHasKey('order_date', $result_array['hits']['hits'][0]['_source']);



        $this->assertArrayHasKey('aggregations', $result_array);
        $this->assertArrayHasKey('test_aggs', $result_array['aggregations']);
    }
}
