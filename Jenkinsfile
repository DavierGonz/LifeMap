pipeline {
    agent any
    options { disableConcurrentBuilds(); skipDefaultCheckout(true) }
    triggers { pollSCM('H/2 * * * *') }
    parameters {
        string(name: 'LOCAL_COMMIT', defaultValue: '', description: 'Solo demostracion local, SHA de bundle preparado')
        choice(name: 'DEPLOY_TARGET', choices: ['azure', 'validate-only', 'local'], description: 'main privado Azure; pruebas locales deben elegir validate-only/local. Publicar solo con autorizacion')
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
                          git fetch -q /var/jenkins_home/local-sources/$LOCAL_COMMIT.bundle HEAD
                          git checkout -q --detach "$LOCAL_COMMIT"
                        '''
                    } else { checkout scm }
                }
            }
        }
        stage('Configuration and isolation') { steps { sh 'bash ci/e2_pipeline.sh prepare' } }
        stage('Images sequential') { steps { sh 'bash ci/e2_pipeline.sh build' } }
        stage('Unit tests gate') { steps { sh 'bash ci/e2_pipeline.sh test' } }
        stage('Pinned temporal and risk models') { steps { sh 'bash ci/e2_pipeline.sh models' } }
        stage('100 real Kafka reviews') { steps { sh 'bash ci/e2_pipeline.sh smoke' } }
        stage('Ten endpoints and UI') { steps { sh 'bash ci/e2_pipeline.sh candidate' } }
        stage('Promotion after all gates') {
            when { expression { params.DEPLOY_TARGET != 'validate-only' } }
            steps { sh 'bash ci/e2_pipeline.sh promote' }
        }
    }
    post {
        always {
            junit testResults: 'reports/pytest.xml', allowEmptyResults: true
            archiveArtifacts artifacts: 'reports/*', allowEmptyArchive: true
            sh 'bash ci/e2_pipeline.sh cleanup'
        }
    }
}
