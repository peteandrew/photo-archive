## Create DynamoDB tables
./bin/aws dynamodb create-table --billing-mode PAY_PER_REQUEST --cli-input-json file://tables/images.json
./bin/aws dynamodb create-table --billing-mode PAY_PER_REQUEST --cli-input-json file://tables/tags_images.json
