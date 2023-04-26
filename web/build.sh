#!/bin/bash
if which node >/dev/null; then 
    echo "node version: "`node -v`
else
    echo "install node"
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
    apt-get install -y nodejs
fi
CURRENT_DIR=$(cd `dirname $0`; pwd)

echo "npm install for api"
cd "$CURRENT_DIR/api"
npm install


echo "npm install for ui"
cd "$CURRENT_DIR/ui"
npm install

echo "npm build for ui"
npm run build
cp -r "$CURRENT_DIR/ui/build" "$CURRENT_DIR/api/website"
