const elasticsearch = require('elasticsearch');
const mysql = require('mysql');

const es_jdbc = require('./lib/pull-push.js');
let pullDataFromMysql = es_jdbc.pull;
let pushDataToElastic = es_jdbc.push;

let transSingleTable = function(pool, client, config, $table, sqlPhrase, $type, filter_map, exception_handler, callback) {
  // 配置项
  let es_config = {
    host: config.elasticsearch.host_config,
    chunkSize: config.elasticsearch.chunkSize,
    timeout: config.elasticsearch.timeout,
    src_table: $table,
    index: config.elasticsearch.index,
    type: $type
  };

  // 导出 mysql 数据表 到 bulk文件
  pullDataFromMysql(pool, $table, sqlPhrase, filter_map, exception_handler, function(obj) {
    if (obj.message == 'success') {
      console.log('==>> ' + obj.table + '.bulk.json 文件构造完毕！');
    }

    // 导入 bulk 数据 到 es
    pushDataToElastic(client, es_config, function(obj) {
      if (obj.message == 'success') {
        console.log('====>> /' + obj.index + '/' + obj.type + ' 导入数据成功！');
        callback(true);
      } else {
        console.log('====>> /' + obj.index + '/' + obj.type + ' 导入数据失败！');
        callback(false);
      }
    });
  });
};

module.exports = function(config, callback) {
  // 开始建立 MySQL 数据库连接
  let pool = mysql.createPool( config.mysql );
  console.log('开始连接数据库：' + config.mysql.host + ':' + config.mysql.port + '/' + config.mysql.database);

  // 开始建立 elasticsearch 客户端服务
  let client = new elasticsearch.Client(config.elasticsearch.host_config);
  console.log('开始建立 elasticsearch 客户端连接...');

  let mapLen = JSON.stringify(config.riverMap).match(/\=\>/g).length;
  let successArr = [];
  let failedArr = [];

  for (let key in config.riverMap) {
    var curTable = key.split('=>')[0].replace(/\s+/g, '');
    var curType = key.split('=>')[1].replace(/\s+/g, '');

    // filter_out
    if (!config.riverMap[key].filter_out || config.riverMap[key].filter_out.length == 0) {
      var filter_map = [];
    } else {
      var filter_map = config.riverMap[key].filter_out;
    }

    // exception_handler
    if (!config.riverMap[key].exception_handler) {
      var exception_handler = {};
    } else {
      var exception_handler = config.riverMap[key].exception_handler;
    }

    // 表名为SQL，则表示将SQL查询结果存入对应type
    let sqlPhrase = !config.riverMap[key].SQL ? '' : config.riverMap[key].SQL;

    transSingleTable(pool, client, config, curTable, sqlPhrase, curType, filter_map, exception_handler, function(state) {
      if (state) {
        successArr.push(curTable);
      } else {
        failedArr.push(curTable);
      }

      if (successArr.length + failedArr.length == mapLen) {
        callback({
          total: mapLen,
          success: successArr.length,
          failed: failedArr.length,
          result: successArr.length == mapLen ? 'success' : 'failed'
        });

        pool.end(function (err) {
          if (err) {
            console.log(err);
          }

          console.log('Mysql 连接已断开...');
        });
      }
    });
  }
};