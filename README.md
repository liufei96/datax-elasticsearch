# datax-elasticsearch
datax的elasticsearch插件，主要是reader插件，writer插件官网已经实现了。适用于es7.x



## 使用步骤

### 1.  下载Datax

[Datax下载地址](http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz)

[Datax的github地址](https://github.com/alibaba/DataX)



将elasticsearchreader工程复制到Datax工程下面

### 2. 修改一些配置

elasticsearchreader工程复制到Datax工程下面。需要修改一些配置



#### 2.1 修改pom.xml

修改Datax的pom.xml。加上elasticsearchreader

```
<module>elasticsearchreader</module>
```



#### 2.2 修改package.xml

修改Datax的package.xml。加上elasticsearchreader

```xml
<fileSet>
    <directory>elasticsearchreader/target/datax/</directory>
    <includes>
        <include>**/*.*</include>
    </includes>
    <outputDirectory>datax</outputDirectory>
</fileSet>
```



## 3. 编译打包

```shell
$ cd  {DataX_source_code_home}
$ mvn -U clean package assembly:assembly -Dmaven.test.skip=true
```

详细内容可参考[官方文档](https://github.com/alibaba/DataX/blob/master/userGuid.md)



## 4. 运行

```
$ cd  {YOUR_DATAX_HOME}/bin
$ python datax.py {YOUR_JOB.json}
```

