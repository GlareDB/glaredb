# ClickBench

Scripts for running the ClickBench benchmark.

## Running on AWS

```sh
aws ec2 run-instances \
  --image-id "ami-084568db4383264d4" \
  --instance-type "c6a.4xlarge" \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":500,"VolumeType":"gp2","DeleteOnTermination":true}}]' \
  --user-data file://aws/userdata.sh
```
