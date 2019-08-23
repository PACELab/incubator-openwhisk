#! /bin/bash
ansible all -i environments/distributed -m ping
ansible-playbook -i environments/distributed setup.yml
echo "Commented docker in prereq_build.yml"
#ansible-playbook -i environments/distributed prereq.yml 
ansible-playbook -i environments/distributed registry.yml
cd ../ && sudo ./gradlew distDocker -PdockerRegistry=serverless-controller:5000
cd ansible
ansible-playbook -i environments/distributed couchdb.yml
ansible-playbook -i environments/distributed initdb.yml
ansible-playbook -i environments/distributed wipe.yml
ansible-playbook -i environments/distributed openwhisk.yml
ansible-playbook -i environments/distributed postdeploy.yml
ansible-playbook -i environments/distributed apigateway.yml
ansible-playbook -i environments/distributed routemgmt.yml
wsk property set --auth $(cat files/auth.guest) --apihost serverless-1
#wsk property set --auth $(cat files/auth.whisk.system) --apihost serverless-controller

#ansible-playbook -i environments/distributed kafka.yml
#ansible-playbook -i environments/distributed controller.yml
#ansible-playbook -i environments/distributed invoker.yml
#ansible-playbook -i environments/distributed edge.yml
#ansible-playbook -i environments/distributed downloadcli.yml
