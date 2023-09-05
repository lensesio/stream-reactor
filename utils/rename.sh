#!/bin/bash
cd "package-rename" || exit
docker-compose up  --force-recreate --build -d; docker-compose up
git add --all
git commit -m "Package rename"
cd "../dir-rename" || exit
docker-compose up  --force-recreate --build -d; docker-compose up
git add --all
git commit -m "Dir rename"
cd "../../"
find . -type d -empty -delete