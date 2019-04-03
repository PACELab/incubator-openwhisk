#! /bin/bash
ansible all -i environments/distributed -m ping
ansible-playbook -i environments/distributed setup.yml
echo "Commented docker in prereq_build.yml"
ansible-playbook -i environments/distributed prereq_build.yml 
ansible-playbook -i environments/distributed registry.yml
cd ../ && sudo ./gradlew distDocker -PdockerRegistry=172.24.66.133:5000
cd ansible
ansible-playbook -i environments/distributed couchdb.yml
ansible-playbook -i environments/distributed initdb.yml
ansible-playbook -i environments/distributed wipe.yml
ansible-playbook -i environments/distributed openwhisk.yml
ansible-playbook -i environments/distributed postdeploy.yml
ansible-playbook -i environments/distributed apigateway.yml
ansible-playbook -i environments/distributed routemgmt.yml
wsk property set --auth $(cat files/auth.whisk.system) --apihost serverless-1
