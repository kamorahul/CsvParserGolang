# Csv Parser with S3 upload and send email

## Process to use it

* Clone the repo
```bash
git clone https://git.clicklabs.in/ClickLabs/RAR-csv.git
```

* Install docker
 Linux :
```bash
sudo apt-get update
sudo apt-get install docker.io
```

* Copy env.list.sample into env.list
```bash
mv env.list.sample env.list
```

* Fill all the credentials in env.list

* Build docker using

```bash
docker build -t csv .
```

* Run Docker by

```bash
 docker run --env-file env.list -it -p 8080:8080 csv
```

## Test

To test simply do curl

```bash
curl localhost:8080/parse
```

> Output "Wrong Method"

## Using it

Sample Request

```bash
curl -X POST \
  http://localhost:8080/parse \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{
	"query": {
		"startDate" : "2017-08-26T07:47:45.850Z",
		"endDate" : "2017-09-09T07:47:45.850Z",
		"type" : "ALL"
	},
	"email" : "kamo.rahul@gmail.com"
}'
```


## Screenshot

Postman Screenshot

![Postman Screenshot](./images/postman.png?raw=true)

## Contribute

* Add more email way
* Add generic support to export collection to CSV









