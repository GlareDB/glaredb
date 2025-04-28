# ClickBench

Scripts for running the ClickBench benchmark.

## Running on AWS

Create instance:

```sh
aws ec2 run-instances \
  --image-id "ami-084568db4383264d4" \
  --instance-type "c6a.4xlarge" \
  --instance-initiated-shutdown-behavior "terminate" \
  --key-name "sean-aws" \
  --block-device-mappings '{"DeviceName":"/dev/sda1","Ebs":{"Encrypted":false,"DeleteOnTermination":true,"SnapshotId":"snap-0edbe0f6601b2861c","VolumeSize":500,"VolumeType":"gp2"}}' \
  --security-group-ids "sg-013d1d33fd81b9117" \
  --count "1"
```

`aws/userdata.sh` will clone glaredb into `/opt/glaredb`.
