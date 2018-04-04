FROM golang

ARG app_env
ENV APP_ENV $app_env

COPY ./ /go/src/github.com/kamorahul/CsvMicroservice/app
WORKDIR /go/src/github.com/kamorahul/CsvMicroservice/app

RUN go get ./
RUN go build -o main .

CMD if [ ${APP_ENV} = production ]; \
        then \
        app; \
        else \
        go get github.com/pilu/fresh && \
        fresh; \
        fi
CMD ["/go/src/github.com/kamorahul/CsvMicroservice/app/main"]
EXPOSE 8080