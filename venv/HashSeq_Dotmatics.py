import requests
import json
import sys
import os
from typing import Callable, Any

class DotmaticsClient():

    def __init__(self, env: str):
        # Internally handles authentication
        self.env = env
        self.username = os.environ.get('DOTMATICS_USERNAME')
        self.password = os.environ.get('DOTMATICS_PASSWORD')
        self.searchParams = self.readSearchParamsFromConfigFile()
        self.setClassVariables()

    def readSearchParamsFromConfigFile(self):
        entry_dict = {}
        file = open('../config.txt', 'r')
        content = file.read()
        entries = content.split("\n")
        for entry in entries:
            p = entry.split("=")
            entry_dict[p[0]] = p[1]
        return entry_dict

    def setClassVariables(self):
        self.HashSeqProjectId = self.searchParams['projectID']
        if self.env == 'Prod':
            self.server = self.searchParams['prodServer']
            self.STUDIES_SUMMARY_HASHSEQ = self.searchParams['prod_StudiesSummaryHashSeq']
            self.CLT_HASH_BIO_TECH = self.searchParams['prod_CltHashBioTech']
            self.CLT_TEST_SAMPLE = self.searchParams['prod_TestSample']
            self.CLT_POOL = self.searchParams['prod_CltPool']
            self.CLT_ASSAY_HASHSEQ = self.searchParams['prod_CltAssayHashSeq']
        else:
            self.server = self.searchParams['testServer']
            self.STUDIES_SUMMARY_HASHSEQ = self.searchParams['test_StudiesSummaryHashSeq']
            self.CLT_HASH_BIO_TECH = self.searchParams['test_CltHashBioTech']
            self.CLT_TEST_SAMPLE = self.searchParams['test_TestSample']
            self.CLT_POOL = self.searchParams['test_CltPool']
            self.CLT_ASSAY_HASHSEQ = self.searchParams['test_CltAssayHashSeq']
            # TODO: Fill in more data sources as needed

    def getHashSeqExperiment(self, experimentId: str):
        datasources = ','.join([
            self.STUDIES_SUMMARY_HASHSEQ,
            self.CLT_HASH_BIO_TECH,
            self.CLT_TEST_SAMPLE,
            self.CLT_POOL,
            self.CLT_ASSAY_HASHSEQ
            # TODO: Fill in more data sources as needed
        ])
        response = requests.get(
            f'{self.server}/browser/api/data/{self.username}/{self.HashSeqProjectId}/{datasources}/{experimentId}?limit=0',
            auth=(self.username, self.password)
        )
        if response.status_code == 200:
            all_data = response.json()
            datasources = all_data[experimentId]['dataSources']
            return {
                'summary': datasources[self.STUDIES_SUMMARY_HASHSEQ],
                'test_sample': datasources[self.CLT_TEST_SAMPLE],
                'tech_data': datasources[self.CLT_HASH_BIO_TECH],
                'pool': datasources[self.CLT_POOL],
                'hashseq': datasources[self.CLT_ASSAY_HASHSEQ]
            }
        else:
            print(f'GET request failed. HTTP status code: {response.status_code}. {response.reason}')
            sys.exit(1)

    def get_sample_csv_input(self, experiment_id: str):
        # Do your request
        request_string = '{}/browser/api/data/{}/{}/{}/{}?limit=0'.format(self.server, self.username,
                                                                          self.HashSeqProjectId,
                                                                          self.CLT_HASH_BIO_TECH, experiment_id)
        response = requests.get(request_string, auth=(self.username, self.password))
        # Check response status_code
        if response.status_code == 200:
            return set(
                ('*', _.get('LIBRARY_ID'), _.get('I7_NAME'))
                for _ in response.json().get(experiment_id).get('dataSources').get(self.CLT_HASH_BIO_TECH).values()
            )
        else:
            print(f'GET request failed. HTTP status code: {response.status_code}. {response.reason}')
            sys.exit(1)

    def _get_datasource_id(self, data_source_name: str):
        request_string = '{}/browser/api/projects/{}'.format(self.server, self.HashSeqProjectId)
        response = requests.get(request_string, auth=(self.username, self.password))
        return next(filter(lambda v: v.get('name') == data_source_name, response.json().get('dataSources').values())) \
            .get('dsID')

    def query_form(self, data_source_name: str, experiment_id: str, filter_: Callable[[Any], Any]):
        data_source_id = self._get_datasource_id(data_source_name)
        request_string = '{}/browser/api/data/{}/{}/{}/{}?limit=0'.format(self.server, self.username, self.HashSeqProjectId,
                                                                      data_source_id, experiment_id)
        response = requests.get(request_string, auth=(self.username, self.password))
        return filter_(response.json().get(experiment_id).get('dataSources').get(str(data_source_id)).values())

if __name__ == '__main__':
    dotmatics_client = DotmaticsClient('Prod')
    result = dotmatics_client.query_form('CLT_POOL_NAMES', '141885', lambda dss: [_.get('HASHTAG_ID') for _ in dss])
    #result = dotmatics_client.getHashSeqExperiment('141885')
    print(json.dumps(result, indent=4))