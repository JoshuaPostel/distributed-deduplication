FROM alpine:3.10.1
# version x.x.x must match cluster
ADD https://storage.googleapis.com/kubernetes-release/release/vx.x.x/bin/linux/amd64/kubectl /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl
EXPOSE 8001
ENTRYPOINT ["/usr/local/bin/kubectl", "proxy"]
