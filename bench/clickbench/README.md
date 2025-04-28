# ClickBench

Scripts for running the ClickBench benchmark.

## Running on AWS

Create instance:

```sh
aws ec2 run-instances \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=clickbench}]' \
  --image-id "ami-084568db4383264d4" \
  --instance-type "c6a.4xlarge" \
  --instance-initiated-shutdown-behavior "terminate" \
  --key-name "sean-aws" \
  --block-device-mappings '{"DeviceName":"/dev/sda1","Ebs":{"Encrypted":false,"DeleteOnTermination":true,"SnapshotId":"snap-0edbe0f6601b2861c","VolumeSize":500,"VolumeType":"gp2"}}' \
  --security-group-ids "sg-083bde52b049ef7a7" \
  --count "1" \
  --user-data file://aws/userdata.sh
```

`aws/userdata`:

- Installs rustup, protoc, build-essential, and gcc (cc)
- Installs stable rust toolchain
- Clones the glaredb repo to `/home/ubuntu/glaredb`

