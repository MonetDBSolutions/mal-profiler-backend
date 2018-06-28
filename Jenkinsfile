pipeline {
    // agent none
    // agent {docker {image 'python:alpine3.6'}}
    agent {dockerfile true}
    stages {
        stage('Build') {
            // agent {docker {image 'python:alpine-3.6'}}
            steps {
                // Download monetdblite
                sh 'wget -O /tmp/monetdblite.tar.gz https://artifactory.zarniwoop.org/downloads/monetdblite-0.2.2.tar.gz'
                sh 'pip install pipenv'
                sh 'pipenv install'
                sh 'pipenv install /tmp/monetdblite.tar.gz'
            }
        }
        stage('Test') {
            steps {
                sh 'pipenv run py.test --verbose --junit-xml test-reports/results.xml'
            }
        }
        stage('Deploy') {
            steps {
                sh 'echo deploying'
            }
        }
    }
}
