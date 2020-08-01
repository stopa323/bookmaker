pip install --target ./package -r reqs.txt
cd ./package

zip -r9 ${OLDPWD}/function.zip .
cd $OLDPWD
zip -u function.zip main.py

aws lambda update-function-code --function-name gt-collect-csgo-events --zip-file fileb://function.zip

rm -f function.zip
rm -rf package/