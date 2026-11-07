pipeline {
    agent any
    options { disableConcurrentBuilds(); skipDefaultCheckout(true); timestamps() }
    triggers { pollSCM('H/2 * * * *') }
    parameters {
        string(name: 'LOCAL_COMMIT', defaultValue: '', description: 'Solo demostracion local, SHA de bundle preparado')
        booleanParam(name: 'FORCE_TEST_FAILURE', defaultValue: false, description: 'Demostrar que pytest impide desplegar')
    }
    stages {
        stage('Checkout') {
            steps {
                script {
                    if (params.LOCAL_COMMIT) {
                        sh '''set -eu
                          case "$LOCAL_COMMIT" in *[!0-9a-f]*|'') exit 2;; esac
                          test "${#LOCAL_COMMIT}" = 40
                          git init -q
                          git fetch -q /var/jenkins_home/local-sources/$LOCAL_COMMIT.bundle main
                          git checkout -q --detach "$LOCAL_COMMIT"
                        '''
                    } else { checkout scm }
                }
            }
        }
        stage('Configuration') { steps { sh 'bash ci/pipeline.sh prepare' } }
        stage('Sequential images') { steps { sh 'bash ci/pipeline.sh build' } }
        stage('pytest gate') { steps { sh 'bash ci/pipeline.sh test' } }
        stage('Infrastructure') { steps { sh 'bash ci/pipeline.sh infrastructure' } }
        stage('Kaggle cache and approved ingestion') {
            steps {
                withCredentials([string(credentialsId: 'lifemap-kaggle', variable: 'KAGGLE_CREDENTIAL')]) {
                    sh 'bash ci/pipeline.sh ingestion'
                }
            }
        }
        stage('Academic results gate') { steps { sh 'bash ci/pipeline.sh results' } }
        stage('Candidate API integration') { steps { sh 'bash ci/pipeline.sh candidate' } }
        stage('Promote local application') { steps { sh 'bash ci/pipeline.sh deploy' } }
    }
    post {
        always {
            junit testResults: 'reports/pytest.xml', allowEmptyResults: true
            archiveArtifacts artifacts: 'reports/*', allowEmptyArchive: true
            sh 'bash ci/pipeline.sh cleanup'
        }
    }
}
