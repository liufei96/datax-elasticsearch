# DataX ElasticSearchReader


---

## 1 快速介绍

[Datax](https://github.com/alibaba/DataX)
读取elasticsearch数据的插件

## 2 实现原理

根据elasticsearch的rest api接口， 使用searchAfter依次查询数据

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
	"job": {
		"setting": {
			"speed": {
				"channel": 1
			}
		},
		"content": [{
			"reader": {
				"name": "elasticsearchreader",
				"parameter": {
					"endpoints": "127.0.0.1:9200",
					"accessId": "XXX",
					"accessKey": "XXX",
					"index": "test",
					"searchType": "dfs_query_then_fetch",
					"search": [{
						"sort": [{
							"id": {
								"order": "asc"
							}
						}]
					}],
					"size": 100,
					"excludes": ["wrapper_traffic"],
					"includes": [],
					"containsId": false
				}
			},
			"writer": {
				"name": "streamwriter",
				"parameter": {
					"print": true,
					"encoding": "UTF-8"
				}
			}
		}]
	}
}
```

#### 3.2 参数说明

* endpoints
  * 描述：ElasticSearch的连接地址
  * 必选：是
  * 默认值：无
  * 可以配置多个，多个之间使用,号隔开。如：127.0.0.1:9200,127.0.0.1:9201

* accessId
  * 描述：http auth中的user
  * 必选：否
  * 默认值：空

* accessKey
  * 描述：http auth中的password
  * 必选：否
  * 默认值：空

* index
  * 描述：elasticsearch中的index名
  * 必选：是
  * 默认值：无
  
* searchType
  * 描述：搜索类型
  * 必选：否
  * 默认值：dfs_query_then_fetch

* search
  * 描述：json格式api搜索数据体
  * 必选：是
  * 默认值：[]
  
  说明：因为查询是使用的searchAfter。所以search条件里面一定要加上sort排序规则
    
* size
  * 描述：每次查询的数量。会覆盖search条件中设置的size
  * 必选：否
  * 默认值：10

* excludes
  * 描述：查询数据，排除指定字段。会覆盖search条件中设置的excludes
  * 必选：否
  * 默认值：[]

* includes
  * 描述：查询数据，选择指定字段。会覆盖search条件中设置的includes
  * 必选：否
  * 默认值：[]

* containsId
  * 描述：是否查询出es的_id
  * 必选：否
  * 默认值：false
  
  说明：如果设置为true。则返回的数据第一个就是_id的值。在进行es数据同步时，如果想使用原来的_id，需要将这个值设置为true


## 4 性能报告

略

## 5 约束限制

* filter使用ognl表达式，根对象为整个table对象，key为column最终写入的名称

## 6 FAQ