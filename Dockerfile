FROM alpine:3.5
COPY bin/pd-server /pd-server
EXPOSE 2379 2380
ENTRYPOINT ["/pd-server"]
