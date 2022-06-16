#!groovy

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
def AGENT_LABEL = env.AGENT_LABEL ?: 'ubuntu'
// =================================================================
// https://cwiki.apache.org/confluence/display/INFRA/Jenkins
// https://cwiki.apache.org/confluence/display/INFRA/Multibranch+Pipeline+recipes
// =================================================================

// general pipeline documentation: https://jenkins.io/doc/book/pipeline/syntax/
pipeline {
    agent {
        node {
            label AGENT_LABEL
        }
    }
    
    environment {
        LANG = 'C.UTF-8'
}

    stages {
        stage('Build') {
            steps {
                sh "gradle build"
              }

            post {
                success {
                    archiveArtifacts '**/target/*.jar'
                }
            }
        }
        
        stage('Test') {
            steps {
                sh "gradle test"
            }

            post {
                always {
                    junit testResults: '**/target/surefire-reports/TEST-*.xml', testDataPublishers: [[$class: 'StabilityTestDataPublisher']]
                }
            }
        }
    }
}
