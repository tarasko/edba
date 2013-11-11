bjam
olddir=`pwd`

tmpname=`mktemp.exe -d edba.XXXXXXXX`
cd $tmpname
git clone git@github.com:tarasko/edba.git
git checkout --orphan gh-pages
rm -rf .
rm '.gitignore'

cp -R $olddir/html/* .
git add .
git commit -m 'Upload edba docs'
git push origin gh-pages

cd $olddir
rm -rf $tmpname
