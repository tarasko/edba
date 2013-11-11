echo 'Invoke bjam two times'
bjam
bjam
olddir=`pwd`

tmpname=`mktemp.exe -d --tmpdir edba.XXXXXXXX`
echo "Use $tmpname as temporary folder"

cd $tmpname

git clone git@github.com:tarasko/edba.git
cd edba

git checkout --orphan gh-pages
git rm -rf .
rm '.gitignore'

cp -r $olddir/html/* .
chmod -R u+rwx *

git add .
git commit -m 'Upload edba docs'
git push -f origin gh-pages

cd $olddir
rm -rf $tmpname

exit 0
