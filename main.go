package main

import (
	"gopkg.in/mgo.v2"
	"log"
	"time"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"strconv"
	"os"
	"encoding/csv"
	"reflect"
	"net/http"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"bytes"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/mattbaird/gochimp"
)

const(
	Token = ""
)


const (
	PENDING = "PENDING"
	ACCEPTED = "ACCEPTED"
	PAYMENT_PENDING = "PAYMENT_PENDING"
	ONGOING = "ONGOING"
	ON_THE_WAY = "ON_THE_WAY"
	CANCELLED_BY_CUSTOMER = "CANCELLED_BY_CUSTOMER"
	CANCELLED_BY_DRIVER = "CANCELLED_BY_DRIVER"
	COMPLETED = "COMPLETED"
	REACHED = "REACHED"
	REJECTED = "REJECTED"
	DRIVER_REJECTED = "REJECTED_BY_DRIVER"
	CANCELLED_BY_ADMIN = "CANCELLED_BY_ADMIN"
	EXPIRED = "EXPIRED"
	REFUNDED = "REFUNDED"
	ANULAR = "ANULAR"
	ALL = "ALL"
	CANCELLED = "CANCELLED"
	TRANSACTION_FAILED = "TRANSACTION_FAILED"
)

type QueryData struct {
	StartDate string `json:"startDate" bson:"startDate"`
	EndDate   string `json:"endDate" bson:"endDate"`
	Type      string `json:"type" bson:"type"`
}

type RequestData struct {
	Query QueryData `json:"query"`
	Email string `json:"email"`
}

type CustomerData struct {
	FirstName string `bson:"firstName" json:"firstName"`
	Email     string `bson:"email" json:"email"`
	Rut       string `bson:"rut" json:"rut"`
}

type DriverData struct {
	CustomerData `bson:"customerData" json:"customerData"`
	Location GeoLocation `bson:"location" json:"location"`
}

type DistributorData struct {
	Name string `json:"name" bson:"name"`
	Id   bson.ObjectId `json:"_id" bson:"_id"`
}

type GeoLocation struct {
	Type        string `json:"-"`
	Coordinates []float64 `json:"coordinates" bson:"coordinates"`
}

type CylinderData struct {
	Weight       int64 `bson:"weight" json:"weight"`
	CylinderCode string `bson:"cylinderCode" json:"cylinderCode"`
	Id           bson.ObjectId `json:"_id" bson:"_id"`
}

var CylinderDataArray []CylinderData

type Cylinder struct {
	Price        int64 `bson:"price" json:"price"`
	IsEmpty      bool `bson:"isEmpty" json:"isEmpty"`
	Quantity     int64 `bson:"quantity" json:"quantity"`
	CylinderId   bson.ObjectId `bson:"cylinderId" json:"cylinderId"`
	CylinderData CylinderData `bson:"CylinderData" json:"CylinderData"`
}

type User struct {
	Id                           bson.ObjectId `bson:"_id" json:"_id"`
	CustomerId                   bson.ObjectId `bson:"customerId" json:"customerId"`
	BookingDate                  string `bson:"bookingDate" json:"bookingDate"`
	BookingMadeBy                string `bson:"bookingMadeBy" json:"bookingMadeBy"`
	DriverId                     bson.ObjectId `bson:"driverId" json:"driverId"`
	Location                     GeoLocation `bson:"location" json:"location"`
	Address                      string `bson:"address" json:"address"`
	Calle                        string `bson:"calle" json:"calle"`
	Comuna                       string `bson:"comuna" json:"comuna"`
	Depto                        string `bson:"depto" json:"depto"`
	Na                           string `bson:"na" json:"na"`
	Region                       string `bson:"region" json:"region"`
	OtherDetails                 string `bson:"otherDetails" json:"otherDetails"`
	ComunaId                     bson.ObjectId `bson:"comunaId" json:"comunaId"`
	BookingStartTime             time.Time `bson:"bookingStartTime" json:"bookingStartTime"`
	BookingEndTime               time.Time `bson:"bookingEndTime" json:"bookingEndTime"`
	LocalBookingStartTime        time.Time `bson:"localBookingStartTime" json:"localBookingStartTime"`
	ReassignmentCreateTime       time.Time `bson:"reassignmentCreateTime" json:"reassignmentCreateTime"`
	LocalBookingEndTime          time.Time `bson:"localBookingEndTime" json:"localBookingEndTime"`
	ActualBookingStartTime       time.Time `bson:"actualBookingStartTime" json:"actualBookingStartTime"`
	ActualBookingEndTime         time.Time `bson:"actualBookingEndTime" json:"actualBookingEndTime"`
	CreatedAt                    time.Time `bson:"createdAt" json:"createdAt"`
	TbkToken                     string `bson:"tbkToken" json:"tbkToken"`
	BuyOrderKey                  string `bson:"buyOrderKey" json:"buyOrderKey"`
	Instructions                 string `bson:"instructions" json:"instructions"`
	PromisedETA                  int64 `bson:"promisedETA" json:"promisedETA"`
	PushSentCount                int64 `bson:"pushSentCount" json:"pushSentCount"`
	PaymentMethod                bson.ObjectId `bson:"paymentMethod" json:"paymentMethod"`
	Customer_paid                bool `bson:"customer_paid" json:"customer_paid"`
	PushesSentTo                 bson.ObjectId `bson:"pushesSentTo" json:"pushesSentTo"`
	ForceFullRequest             bson.ObjectId `bson:"forcefullRequest" json:"forcefullRequest"`
	FirstDriverAssigned          bson.ObjectId `bson:"firstDriverAssigned" json:"firstDriverAssigned"`
	Status                       string `bson:"status" json:"status"`
	CustomerNameIfOffline        string `bson:"customerNameIfOffline" json:"customerNameIfOffline"`
	CustomerNoIfOffline          string `bson:"customerNoIfOffline" json:"customerNoIfOffline"`
	Requests                     bson.ObjectId `bson:"requests" json:"requests"`
	IfAssigned                   bool `bson:"ifAssigned" json:"ifAssigned"`
	IsPrivate                    bool `bson:"isPrivate" json:"isPrivate"`
	IdPedido                     string `bson:"idPedido" json:"idPedido"`
	SatId                        string `bson:"satId" json:"satId"`
	TotalAmount                  int64 `bson:"totalAmount" json:"totalAmount"`
	GrandTotal                   int64 `bson:"grandTotal" json:"grandTotal"`
	DistributorId                bson.ObjectId `bson:"distributorId" json:"distributorId"`
	Cylinders                    []Cylinder `bson:"cylinders" json:"cylinders"`
	Patent                       string `bson:"patent" json:"patent"`
	TransactionId                string `bson:"transactionId" json:"transactionId"`
	CreditsUsed                  int64 `bson:"creditsUsed" json:"creditsUsed"`
	CompletedAt                  string `bson:"completedAt" json:"completedAt"`
	AcceptedAt                   time.Time `bson:"acceptedAt" json:"acceptedAt"`
	CancelledAt                  string `bson:"cancelledAt" json:"cancelledAt"`
	Is_deleted                   bool `bson:"is_deleted" json:"is_deleted"`
	RatingByCustomer             int64 `bson:"ratingByCustomer" json:"ratingByCustomer"`
	RatingByDriver               int64 `bson:"ratingByDriver" json:"ratingByDriver"`
	FeedbackByCustomer           string `bson:"feedbackByCustomer" json:"feedbackByCustomer"`
	FeedbackByDriver             string `bson:"feedbackByDriver" json:"feedbackByDriver"`
	CancellationReasonByCustomer string `bson:"cancellationReasonByCustomer" json:"cancellationReasonByCustomer"`
	CancellationReasonByDriver   string `bson:"cancellationReasonByDriver" json:"cancellationReasonByDriver"`
	Admin_assigned               bool `bson:"admin_assigned" json:"admin_assigned"`
	IsAnulled                    bool `bson:"isAnulled" json:"isAnulled"`
	CustomerData                 []CustomerData `bson:"customerData" json:"customerData"`
	DriverData                   []DriverData `bson:"driverData" json:"driverData"`
	RequestData                  map[string]interface{} `bson:"requestData" json:"requestData"`
	DistributorData              []DistributorData `json:"distributorData" bson:"distributorData"`
}

func GenerateCsv(requestData RequestData) {
	/*
	Mongo Connection
	*/
	mongoUri := os.Getenv("MONGOURI")
	session, err := mgo.Dial(mongoUri)
	if err != nil {
		log.Fatal(err)
	}

	/*Instance of Orders
	*/
	collection := session.DB(os.Getenv("DATABASE")).C(os.Getenv("COLLECTION"));

	var result  []User
	fileName := strconv.FormatInt(int64(time.Now().Nanosecond()), 10) + ".xls"
	file, err := os.Create( fileName)
	timeNow := time.Now()
	fmt.Println(timeNow)
	//err = collection.Find(bson.M{ "bookingDate":{ "$lte": "2017-11-14T18:30:00.000Z", "$gte": "2017-09-13T18:30:00.000Z" }, "status":{ "$in":[]string {"CANCELLED_BY_CUSTOMER", "CANCELLED_BY_DRIVER", "CANCELLED_BY_ADMIN", "PENDING", "EXPIRED", "ACCEPTED", "REACHED" , "COMPLETED", } }, "isAnulled": { "$exists": false } }).
	fmt.Println(requestData.Query.StartDate)
	queryData := bson.M{}

	if (requestData.Query.StartDate != "" && requestData.Query.EndDate != "") {
		layout := "2006-01-02T15:04:05.000Z"
		endDate,err := time.Parse(layout,requestData.Query.EndDate)
		startDate,err := time.Parse(layout,requestData.Query.StartDate)
		if(err != nil){
			log.Fatal(err);
		}
		queryData["bookingDate"] = bson.M{"$lte": time.Date(endDate.Year(),endDate.Month(),endDate.Day(),endDate.Hour(),endDate.Minute(),endDate.Second(),0,time.UTC), "$gte": time.Date(startDate.Year(),startDate.Month(),startDate.Day(),startDate.Hour(),startDate.Minute(),startDate.Second(),0,time.UTC) }
	}

	if (requestData.Query.Type == ALL) {
		listData := []string{
			CANCELLED_BY_CUSTOMER,
			CANCELLED_BY_DRIVER,
			CANCELLED_BY_ADMIN,
			PENDING,
			EXPIRED,
			ACCEPTED,
			REACHED,
			COMPLETED,
		}

		queryData["status"] = bson.M{"$in" : listData, }
		queryData["isAnulled"] = bson.M{"$exists": false}
	} else if(requestData.Query.Type == CANCELLED){
		listDataCanceled := []string{
			CANCELLED_BY_CUSTOMER,
			CANCELLED_BY_DRIVER,
			CANCELLED_BY_ADMIN,
		}

		queryData["status"] = bson.M{"$in" : listDataCanceled, }
		queryData["isAnulled"] = bson.M{"$exists": false}
	} else if(requestData.Query.Type == ANULAR){
		queryData["isAnulled"] = true

	} else {
		queryData["status"] = requestData.Query.Type
		queryData["isAnulled"] = bson.M{"$exists": false}
	}
	//queryData[""]
	fmt.Println(queryData)
	pipeline := []bson.M{
		bson.M{"$match" : queryData},
		bson.M{"$lookup": bson.M{"from": "cylinders", "localField": "cylinders.cylinderId", "foreignField": "_id", "as": "cylinders.cylinderData"}},
		bson.M{"$lookup": bson.M{"from": "customers", "localField": "customerId", "foreignField": "_id", "as": "customerData"}},
		bson.M{"$lookup": bson.M{"from": "serviceproviders", "localField": "driverId", "foreignField": "_id", "as": "driverData"}},
		bson.M{"$lookup": bson.M{"from": "distributors", "localField": "distributorId", "foreignField": "_id", "as": "distributorData"}},
	}
	//err = collection.Find(bson.M{}).
	//	All(&result)

	pipe := collection.Pipe(pipeline)
	pipe.All(&result)
	w := csv.NewWriter(file)
	defer w.Flush()
	//var resultString []string
	timeBetween := time.Now()
	fmt.Println(timeBetween)
	counter := 0
	var str = []string{"ID\t", "ID Pedido\t" , "SAT ID\t" , "Fecha/Hora Pedido Confirmado\t" , "Fecha/Hora Pedido Aceptado\t" , "Fecha/Hora Pedido Completado\t" , "Fecha/Hora Pedido Asignado\t" , "Status Pedido\t" , "Cliente Nombre\t" , "Cliente Email\t" , "Cliente Rut\t" , "Chofer Nombre\t" , "Chofer Rut\t" , "Chofer Patent\t" , "ID Distribuidror\t" , "Nombre Distribuidor\t" , "Dirección Pedido\t" , "Comuna Pedido\t" , "Detalle del Pedido\t" , "Monto Total Pedido\t" , "Kilos Total Pedido\t" , "Método de Pago\t" , "Motivo Cancelación\t" , "Comentarios Cliente\t" , "Evaluación Cliente\t" , "Comentarios Chofer\t" , "Evaluación Chofer\t" , "Promesa Tiempo Despacho Pedido\t" , "Origen Pedido\t" , "Tipo Pedido\t" , "GPS LAT Cliente\t" , "GPS LONG Cliente\t" , "GPS Lat Chofer\t" , "GPS LONG Chofer\t" , "Radio Búsqueda Pedido\t\n"}
	w.Write(str)
	for _, record := range result {
		counter++;
		json.Marshal(&record)
		//fmt.Println(record)
		var distance string
		if (record.RequestData["requests"] != nil) {
			var requestArray = record.RequestData["requests"]
			switch reflect.TypeOf(requestArray).Kind() {
			case reflect.Slice:
				s := reflect.ValueOf(requestArray)
				for i := 0; i > s.Len(); i++ {
					item := s.Index(i).Interface().(map[string]interface{});

					if (item["driverAccepted"] != nil) {
						driverAccepted := item["driverAccepted"].(map[string]interface{})
						distance = driverAccepted["distance"].(string)
					}
				}
			}
		}
		var totalWeight int64 = 0;
		details := ""
		if (record.Cylinders != nil) {
			for _, cylinder := range record.Cylinders {
				var storedCylinderData CylinderData
				for _, storedCylinder := range CylinderDataArray {
					if (storedCylinder.Id.Hex() == cylinder.CylinderId.Hex()) {
						storedCylinderData = storedCylinder
						break;
					}
				}
				//weight,_ := strconv.ParseInt(cylinder.CylinderData.Weight,10,0)
				totalWeight += storedCylinderData.Weight
				details += "CilindroCódigo" + ":" + storedCylinderData.CylinderCode + "," + "Cilindro Peso" + ":" + strconv.FormatInt(storedCylinderData.Weight, 10) + "," + "cantidad" + ":" + strconv.FormatInt(cylinder.Quantity, 10) + "," + "Precio" + ":" + "$" + strconv.FormatInt(cylinder.Price, 10) + ";"
			}
		}
		var bookingOrigin string
		if (record.BookingMadeBy != "") {
			if (record.IdPedido != "") {
				bookingOrigin = "offline"
			} else {
				bookingOrigin = "online"
			}
		}
		var bookingType string
		if (record.IsPrivate) {
			bookingType = "private"
		} else {
			bookingType = "public"
		}
		CustFirstName := ""
		CustEmail := ""
		CustRut := ""
		DriverFirstName := ""
		DriverRut := ""
		if (len(record.CustomerData) > 0) {
			CustFirstName = record.CustomerData[0].FirstName
			CustEmail = record.CustomerData[0].Email
			CustRut = record.CustomerData[0].Rut

		}

		DistributorId := ""
		DistributorName := ""
		if (len(record.DistributorData) > 0) {
			DistributorId = record.DistributorData[0].Id.Hex()
			DistributorName = record.DistributorData[0].Name
		}

		OrderLat := 0.0
		OrderLng := 0.0
		if (len(record.Location.Coordinates) > 0) {
			if (len(record.Location.Coordinates) > 0) {
				OrderLat = record.Location.Coordinates[0]
				OrderLng = record.Location.Coordinates[1]
			}
		}
		DriverLat := 0.0
		DriverLng := 0.0
		if (len(record.DriverData) > 0) {
			DriverFirstName = record.DriverData[0].FirstName
			DriverRut = record.DriverData[0].Rut
			if (len(record.DriverData[0].Location.Coordinates) > 1) {
				DriverLat = record.DriverData[0].Location.Coordinates[0]
				DriverLng = record.DriverData[0].Location.Coordinates[1]
			}
		}

		var strCells = []string{
			record.Id.Hex() + "\t" ,
				record.IdPedido + "\t" ,
				record.SatId + "\t" ,
				GetSpanishDate(record.BookingStartTime) + "\t" ,
				GetSpanishDate(record.AcceptedAt) + "\t" ,
				GetSpanishDate(record.BookingEndTime) + "\t" ,
				GetSpanishDate(record.ReassignmentCreateTime) + "\t" ,
				record.Status + "\t" ,
				CustFirstName + "\t" ,
				CustEmail + "\t" ,
				CustRut + "\t" ,
				DriverFirstName + "\t" ,
				DriverRut + "\t" ,
				record.Patent + "\t" ,
				DistributorId + "\t" ,
				DistributorName + "\t" ,
				record.Address + "\t" ,
				record.Comuna + "\t" ,
				details + "\t" , //TODO Full details capture
				strconv.FormatInt(record.GrandTotal, 10) + "\t" ,
				strconv.FormatInt(totalWeight, 10) + "\t" ,
				record.PaymentMethod.Hex() + "\t" ,
				record.CancellationReasonByDriver + "\t" ,
				record.CancellationReasonByCustomer + "\t" ,
				record.FeedbackByCustomer + "\t" ,
				record.FeedbackByDriver + "\t" ,
				strconv.FormatInt(record.RatingByDriver,10) + "\t" ,
				strconv.FormatInt(record.PromisedETA,10) + "\t" ,
				bookingOrigin + "\t" ,
				bookingType + "\t" ,
				strconv.FormatFloat(OrderLat, 'g', 6, 64) + "\t" ,
				strconv.FormatFloat(OrderLng, 'g', 6, 64) + "\t" ,
				strconv.FormatFloat(DriverLat, 'g', 6, 64) + "\t" ,
				strconv.FormatFloat(DriverLng, 'g', 6, 64) + "\t" ,
				distance + "\t\n",
		}
		w.Write(strCells)
	}

	timeAfter := time.Now()
	fmt.Println(timeAfter)
	fmt.Println(timeAfter.Sub(timeNow))
	fmt.Println("Counted", counter)
	UploadToS3(fileName,requestData.Email)

	defer session.Close()
	//goGroup.Done()
}


func GetSpanishDate(ts time.Time) string{
	location,err := time.LoadLocation("Chile/Continental");
	if err != nil{
		log.Fatal("Issue converting time")
	}
	return ts.In(location).Format("02/01/2006 15:04:05 MST")
}
//func TestMongo() {
//	session, err := mgo.Dial("mongodb://localhost:27017")
//	defer session.Close()
//	if err != nil {
//		fmt.Println(err)
//	}
//	collection := session.DB("test").C("test");
//	resp := bson.M{}
//	collection.Pipe([]bson.M{bson.M{"$match" : bson.M{"_id" : bson.ObjectIdHex("58e66d4d3c85edff56a6f423")}},
//		bson.M{"$lookup": bson.M{"from": "cylinders", "localField": "cylinders.cylinderId", "foreignField": "_id", "as": "cylinders.cylinderData"}},
//		bson.M{"$lookup": bson.M{"from": "customers", "localField": "customerId", "foreignField": "_id", "as": "customerData"}},
//		bson.M{"$lookup": bson.M{"from": "serviceproviders", "localField": "driverId", "foreignField": "_id", "as": "driverData"}},
//		bson.M{"$lookup": bson.M{"from": "distributors", "localField": "distributorId", "foreignField": "_id", "as": "distributorData"}},
//	}).One(&resp)
//	fmt.Println(resp)
//
//}

func UploadToS3(fileName string, email string) string{
	AwsAccessKey := os.Getenv("AWSACCESSKEY");
	AwsSecret := os.Getenv("AWSSECRET");
	bucketName := os.Getenv("BUCKETNAME")
	regionName := os.Getenv("REGION")
	creds := credentials.NewStaticCredentials(AwsAccessKey,AwsSecret,Token)

	_,err := creds.Get();
	if(err != nil){
		log.Fatal(err)
	}

	cfg := aws.NewConfig().WithRegion(regionName).WithCredentials(creds);

	svc := s3.New(session.New(),cfg);

	file,err := os.Open(fileName)

	if err != nil {
		fmt.Printf("err opening file: %s", err)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()


	size := fileInfo.Size()

	buffer := make([]byte, size) // read file content to buffer

	file.Read(buffer)
	fileBytes := bytes.NewReader(buffer)
	fileType := http.DetectContentType(buffer)

	path := "/uploads/" + file.Name()

	params := &s3.PutObjectInput{
		ACL:aws.String("public-read"),
		Bucket: aws.String(bucketName),
		Key: aws.String(path),
		Body: fileBytes,
		ContentLength: aws.Int64(size),
		ContentType: aws.String(fileType),
		Metadata: map[string]*string{
			"metadata1": aws.String("value1"),
			"metadata2": aws.String("value2"),
		},
	}
	resp, err := svc.PutObject(params)
	if err != nil {
		fmt.Printf("bad response: %s", err)
	}
	fmt.Printf("resp onse %s", awsutil.StringValue(resp))
	SendViaEmail(email,"https://s3."+ regionName +".amazonaws.com/"+ bucketName +"/uploads/" + fileName)
	return ""
}

func SendViaEmail(email string ,url string){
	apiKey := os.Getenv("MANDRILL_KEY")
	//apiKey := "YX8a1OKQdLGqDMJ9xNkPEg"
	mandrillApi, err := gochimp.NewMandrill(apiKey)
	if err != nil {
		fmt.Println("Error instantiating client")
	}
	recipients := []gochimp.Recipient{
		gochimp.Recipient{Email: email},
	}
	message := gochimp.Message{
		Html:      "Hi Team,View the attachment of booking data <br><br> " + url,
		Subject:   "Hi ,CSV of booking documents please check",
		FromEmail: os.Getenv("FROM_EMAIL"),
		FromName:  os.Getenv("FROM_NAME"),
		To:       recipients ,
	}
	_,err = mandrillApi.MessageSend(message,false);
	if(err != nil ){
		fmt.Println(err);
	}


}

func main() {
	//goGroup := new(sync.WaitGroup)
	//SendViaEmail("kamo.rahul@gmail.com","https://s3.ap-south-1.amazonaws.com/packnorder/media/sample1.xls")
	http.HandleFunc("/parse", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			requestData := json.NewDecoder(r.Body)
			var decodedData RequestData
			requestData.Decode(&decodedData);
			defer r.Body.Close()
			fmt.Println(decodedData)
			go GenerateCsv(decodedData)
			w.Write([]byte("In Progress"))

		} else {
			w.Write([]byte("Wrong Method"))
		}
	})
	//goGroup.Add(20)
	//goGroup.Wait()
	fmt.Println("Initiating Listner with Port" + os.Getenv("PORT"))
	err :=  http.ListenAndServe(":" + os.Getenv("PORT"), nil)
	if(err != nil){
		log.Fatal(err);
	}
}


