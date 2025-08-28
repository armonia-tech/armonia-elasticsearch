<?php
namespace ArmoniaElasticSearch;

use Elastic\Elasticsearch\ClientBuilder;
use Elastic\Elasticsearch\Client;

class ElasticSearchEngine
{
    private Client $elasticSearchClient;

    /**
     * Construct
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  array $settings
     * @return void
     */
    public function __construct(array $settings)
    {
        $this->elasticSearchClient = ClientBuilder::create()
            ->setHosts($settings['hosts'])
            ->setRetries($settings['retry'])
            ->build();
    }

    /**
     * Get Index List
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @return void
     */
    public function getIndexList(string $indexName)
    {
        $params   = [
            'index' => $indexName,
        ];

        return $this->elasticSearchClient->cat()->indices($params);
    }

    /**
     * Create Index
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $settings
     * @param  array $mappings
     * @return void
     */
    public function createIndex(string $indexName, array $settings = [], array $mappings = [])
    {
        $params   = [
            'index' => $indexName,
            'body'  => [
                'settings' => (!empty($settings))? $settings : $this->_getDefaultIndexSettings()
            ]
        ];

        if (!empty($mappings)) {
            $params['body']['mappings'] = $mappings;
        }

        $this->elasticSearchClient->indices()->create($params);
    }

    /**
     * Delete Index
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @return void
     */
    public function deleteIndex(string $indexName)
    {
        $params   = [
            'index' => $indexName
        ];

        $this->elasticSearchClient->indices()->delete($params);
    }

    /**
     * Get Alias
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $alias
     * @return array
     */
    public function getAlias(string $alias)
    {
        $params   = [
            'name' => $alias
        ];

        try {
            $output = $this->elasticSearchClient->indices()->getAlias($params);
        } catch (\Exception $e) {
            if ($e->getCode() == 404) {
                return [];
            }

            throw new \Exception($e);
        }

        return $output;
    }

    /**
     * Add Alias
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  string $alias
     * @return void
     */
    public function addAlias(string $indexName, string $alias)
    {
        $params   = [
            'body'  => [
                'actions' => [
                    [
                        'add' => [
                            'index' => $indexName,
                            'alias' => $alias
                        ]
                    ]
                ]
            ]
        ];

        $this->elasticSearchClient->indices()->updateAliases($params);
    }

    /**
     * Remove Alias
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  string $alias
     * @return void
     */
    public function removeAlias(string $indexName, string $alias)
    {
        $params   = [
            'body'  => [
                'actions' => [
                    [
                        'remove' => [
                            'index' => $indexName,
                            'alias' => $alias
                        ]
                    ]
                ]
            ]
        ];

        $this->elasticSearchClient->indices()->updateAliases($params);
    }

    /**
     * Exist Alias
     *
     * @author Shin Shen <shinshen.yeoh@armonia-tech.com>
     * @param string $aliasName
     * @return bool
     */
    public function existAlias(string $aliasName): bool
    {
        $params = [
            'name' => $aliasName
        ];

        return $this->elasticSearchClient->indices()->existsAlias($params);
    }

    /**
     * Get Mapping
     *
     * @author Shin Shen <shinshen.yeoh@armonia-tech.com>
     * @param string $indexName
     * @return array
     */
    public function getMapping(string $indexName)
    {
        $params = [
            'index' => $indexName,
        ];

        return $this->elasticSearchClient->indices()->getMapping($params);
    }

    /**
     * Put Mapping
     *
     * @author Shin Shen <shinshen.yeoh@armonia-tech.com>
     * @param string $indexName
     * @param array $mappingBody
     * @return void
     */
    public function putMapping(string $indexName, array $mappingBody)
    {
        $params = [
            'index' => $indexName,
            'body' => $mappingBody,
        ];

        $this->elasticSearchClient->indices()->putMapping($params);
    }

    /**
     * Put Settings
     *
     * @author Wilson <huanyong.chan@armonia-tech.com>
     * @param string $indexName
     * @param array $settingBody
     * @return void
     */
    public function putSettings(string $indexName, array $settingBody)
    {
        // remove settings doesnt allow to update
        unset($settingBody['number_of_shards']);

        $params = [
            'index' => $indexName,
            'body' => $settingBody,
        ];

        $this->elasticSearchClient->indices()->close(['index' => $indexName]);
        $this->elasticSearchClient->indices()->putSettings($params);
        $this->elasticSearchClient->indices()->open(['index' => $indexName]);
    }

    /**
     * Add / Update Doc
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $record
     * @param  string $id default ''
     * @param  string $idField default 'id'
     * @return void
     */
    public function addDoc(string $indexName, array $record = [], string $id = "", string $idField = 'id')
    {
        $params = [
            'index' => $indexName
        ];

        //add / update document
        if (empty($id)) {
            $params['id']   = $record[$idField];
            $params['body'] = $record;

            $this->elasticSearchClient->index($params);
        } else {
            $params['id']          = $id;
            $params['body']['doc'] = $record;

            $this->elasticSearchClient->update($params);
        }
    }

    /**
     * Add Docs
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $records
     * @param  string $idField default 'id'
     * @return array
     */
    public function addDocs(string $indexName, array $records = [], string $idField = 'id')
    {
        if (!count($records)) {
            throw new \Exception('Elasticsearch.addDocs.noDocs: An empty records array was passed to the addDocs() method.');
        }

        if (count($records)) {
            for ($i = 0; $i < count($records); $i++) {
                $params['body'][] = [
                    'index' => [
                        '_index' => $indexName,
                        '_id'    => $records[$i][$idField]
                    ]
                ];

                $params['body'][] = $records[$i];
            }

            return $this->elasticSearchClient->bulk($params);
        }
    }

    /**
     * Delete Doc
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  string $id
     * @param  bool|string $refresh optional default false, value=true,false,wait_for
     * @return void
     */
    public function deleteDoc(string $indexName, string $id, $refresh = false)
    {
        $params = [
            'index' => $indexName,
            'id'    => $id
        ];

        if ($refresh !== false) {
            $params['refresh'] = $refresh;
        }

        return $this->elasticSearchClient->delete($params);
    }

    /**
     * Delete Doc By Query
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $body
     * @return void
     */
    public function deleteByQuery(string $indexName, array $body)
    {
        $params = [
            'index' => $indexName,
            'body'  => $body
        ];

        return $this->elasticSearchClient->deleteByQuery($params);
    }

    /**
     * Search
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array  $query
     * @param  int $from
     * @param  int $size
     * @param  array $sort
     * @param  array $aggs
     * @param  string $scroll
     * @param  array $search_after
     * @return array
     */
    public function search(
        string $indexName,
        array $source = [],
        array  $query = [],
        int $from = 0,
        int $size = 10,
        array $sort = ["_score"],
        array $aggs = [],
        string $scroll = '',
        array $search_after = [],
        array $pit = []
    ) {
        $body = [
            'size'  => $size,
            'sort'  => $sort,
            'track_total_hits' => true
        ];

        if (!empty($from)) {
            $body['from'] = $from;
        }

        if (!empty($query)) {
            $body['query'] = $query;
        }

        if (!empty($aggs)) {
            $body['aggs'] = $aggs;
        }

        if (!empty($source)) {
            $body['_source'] = $source;
        }

        if (!empty($search_after)) {
            $body['search_after'] = $search_after;
        }

        $params = [
            'index' => $indexName,
            'body'  => $body
        ];

        if (!empty($scroll)) {
            $params['scroll'] = $scroll;
        }

        if (!empty($pit)) {
            $params['pit'] = $pit;
        }

        return $this->elasticSearchClient->search($params);
    }

    /**
     * Open Point In Time
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html
     * @author Shin Shen <shinshen.yeoh@armonia-tech.com>
     * @param array  $params
     */
    public function openPointInTime(string $index_name, string $keep_alive, array $params = []) {
        $params['index'] = $index_name;
        $params['keep_alive'] =$keep_alive;
        return $this->elasticSearchClient->openPointInTime($params)->asArray();
    }

    /**
     * Scroll
     *
     * Remark: currently only support passing scroll_id. All customizations are
     * disabled
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-body.html#request-body-search-scroll
     * @param string $scroll_id
     * @return array
     */
    public function scroll(string $scroll_id) {
        $params = ['scroll_id' => $scroll_id];
        return $this->elasticSearchClient->scroll($params);
    }

    /**
     * Is Elastic Search Available
     *
     * Check if elastic search is available by using ping API.
     *
     * Return type is always bool based on internal client spec but exception
     * will be thrown if there are failure.
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @return bool true if success
     * @throws Exception
     */
    public function isElasticSearchAvailable(): bool
    {
        $params = [];

        return $this->elasticSearchClient->ping($params);
    }

    private function _getDefaultIndexSettings()
    {
        return [
            'number_of_shards'   => 1,
            'number_of_replicas' => 0
        ];
    }
}
