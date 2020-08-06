#code for taking the password of he user from the SSM component of AWS

 ssm = boto3.client('ssm',region_name = '$region')
 parameter = ssm.get_parameter(Name = '$path_to_the_location'+ $user, WithDecryption = True
 password = parameter['Parameter']['Value']
 
 
 #Code for getting the password from the Talend-ETL
 "aws ssm get-parameter --name $path_to_the_location"+(String)((Map)context.configuration).get("audit_user")+" --with-decryption --region $region --output text --query Parameter.Value"
