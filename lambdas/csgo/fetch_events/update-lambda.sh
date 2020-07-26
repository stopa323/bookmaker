pip install --target ./package -r reqs.txt
cd ./package

zip -r9 ${OLDPWD}/function.zip .
cd $OLDPWD
zip -u function.zip efortuna_main.py

aws lambda update-function-code --function-name collect-csgo-events-url --zip-file fileb://function.zip

rm -f function.zip
rm -rf package/