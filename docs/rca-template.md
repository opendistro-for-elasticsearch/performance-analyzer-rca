# Name of RCA


## Small 2 line Summary

This RCA is used to detect bottlenect in resource X by ..

## Usecases
- usecase 1
- usecase 2

## Possible remediation actions
- action1 
- action2


## Structure

### Input Metrics

1. metric1
2. metric2
3. ..

### Output When the resource is healthy

#### Relational format as stored in SQLite with column names

 <table style="width:100%">
  <tr>
    <th>Column1</th>
    <th>Column2</th>
    <th>Column3</th>
    <th> ...</th>
  </tr>
  <tr>
    <td>value11</td>
    <td>value12</td>
    <td>value13</td>
    <td>..</td>
  </tr>
  <tr>
    <td>value21</td>
    <td>value22</td>
    <td>value23</td>
    <td>..</td>
  </tr>
</table> 

#### Rest request format
TBD

#### Rest response format

```
{
	"key1": "value1",
	"key2": {
		"key3": "value3"
	},
	"key4": value4
}
```

### Output when the resource is un-healthy

#### Relational format as stored in SQLite with column names


 <table style="width:100%">
  <tr>
    <th>Column1</th>
    <th>Column2</th>
    <th>Column3</th>
    <th> ...</th>
  </tr>
  <tr>
    <td>value11</td>
    <td>value12</td>
    <td>value13</td>
    <td>..</td>
  </tr>
  <tr>
    <td>value21</td>
    <td>value22</td>
    <td>value23</td>
    <td>..</td>
  </tr>
</table> 


#### Rest request format
TBD

#### Rest response format

```
{
	"key1": "value1",
	"key2": {
		"key3": "value3"
	},
	"key4": value4
}
```

## RCA graph images

Should include the graph nodes and on which ES nodes will they run.

1. Please use a three data node, three master node cluster.
2. Please 3 es nodes with colocated master and data

## Descriptive summary



