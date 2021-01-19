#! /bin/sh
if [ $# != 1 ]
then
    echo "Pas assez d'arguments : $1\n"
    echo "Usage : ./authenticate.sh usernameN7"
else
    ssh-keygen -t  rsa
    ssh-copy-id $1@tao.enseeiht.fr
fi