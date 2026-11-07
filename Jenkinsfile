pipeline {
    agent any
    options { disableConcurrentBuilds(); skipDefaultCheckout(true) }
    triggers { pollSCM('H/2 * * * *') }
    parameters {
        string(name: 'LOCAL_COMMIT', defaultValue: '', description: 'Solo demostracion local, SHA de bundle preparado')
        booleanParam(name: 'DEPLOY_PUBLIC', defaultValue: true, description: 'Promover solo tras gates; false para preflight')
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
        stage('Configuration') { steps { sh 'bash ci/azure_pipeline.sh prepare' } }
        stage('API images sequential') { steps { sh 'bash ci/azure_pipeline.sh build' } }
        stage('pytest gate') { steps { sh 'bash ci/azure_pipeline.sh test' } }
        stage('Existing results') { steps { sh 'bash ci/azure_pipeline.sh verify' } }
        stage('Kaggle credential') {
            steps {
                withCredentials([string(credentialsId: 'lifemap-kaggle', variable: 'KAGGLE_CREDENTIAL')]) {
                    sh 'bash ci/azure_pipeline.sh kaggle'
                }
            }
        }
        stage('Candidate API integration') { steps { sh 'bash ci/azure_pipeline.sh candidate' } }
        stage('Promote Azure API') {
            when { expression { params.DEPLOY_PUBLIC } }
            steps { sh 'bash ci/azure_pipeline.sh deploy' }
        }
    }
    post {
        always {
            junit testResults: 'reports/pytest.xml', allowEmptyResults: true
            archiveArtifacts artifacts: 'reports/*', allowEmptyArchive: true
            sh 'bash ci/azure_pipeline.sh cleanup'
        }
    }
}
