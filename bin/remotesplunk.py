#!/usr/bin/env python
# coding=utf-8
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, unicode_literals
import app
import sys
import time
import io
import re
import splunklib.client as client
import splunklib.results as results
from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
from splunk.clilib import cli_common as cli

class ResponseReaderWrapper(io.RawIOBase):
  def __init__(self, responseReader):
    self.responseReader = responseReader
 
  def readable(self):
    return True
 
  def close(self):
    self.responseReader.close()
 
  def read(self, n):
    return self.responseReader.read(n)
 
  def readinto(self, b):
    sz = len(b)
    data = self.responseReader.read(sz)
    for idx, ch in enumerate(data):
      b[idx] = ch
    return len(data)

@Configuration(type='reporting', distributed=False)
class GenerateRemoteSplunkCommand(GeneratingCommand):
    search = Option(require=True)
    instancename = Option(require=True)
 
    def generate(self):
        cfg = cli.getConfStanza('remotespl',self.instancename)
        uri = cfg.get('uri')
        username = cfg.get('username')
        sslverify = bool(int(cfg.get('sslverify')))

        match = re.search('^(https?):\/\/([^:]+):([0-9]+)', uri) 

        if match:
          scheme = match.group(1)
          host = match.group(2)
          port = match.group(3)
        else:
          raise Exception("ERROR: uri value " + str(uri) + " is not valid")

        storage_passwords=self.service.storage_passwords
        foundcreds = 0
        for credential in storage_passwords:
          if credential.content.get('realm') == self.instancename and credential.content.get('username') == username:
            foundcreds = 1
            password = credential.content.get('clear_password')
            #usercreds = {'realm':credential.content.get('realm'),'username':credential.content.get('username'),'password':credential.content.get('clear_password')}
            #yield usercreds

        if foundcreds == 0:
          raise Exception("ERROR: could not find credentials for instance=" + self.instancename + " username=" + username)

        kwargs_connect={}
        kwargs_connect['scheme'] = scheme
        kwargs_connect['host'] = host
        kwargs_connect['port'] = port
        kwargs_connect['verify'] = sslverify
        kwargs_connect['username'] = username
        kwargs_connect['password'] = password
        kwargs_connect['app'] = "-"
        kwargs_connect['owner'] = "-"

        #Get the earliest and latest time from the search bar
        search_info = self.search_results_info

        service = client.connect(**kwargs_connect)

        search_spl=self.search

        kwargs_search={}
        kwargs_search["earliest_time"]=search_info.search_et
        kwargs_search["latest_time"]=search_info.search_lt
        #kwargs_search["preview"]=True
        remotesearch = service.jobs.create(search_spl, **kwargs_search)

        while not remotesearch.is_ready():
          time.sleep(1)

        kwargs_results={}
        kwargs_results['count']=0
        job = client.Job(service, remotesearch["sid"], **kwargs_results)

        while not job.is_done():
          time.sleep(1)

        count = 0
        while count < int(job["resultCount"]):
          self.logger.debug("Fetching events at offset %s" % count)
          kwargs_results['offset']=count
          result_stream=job.results(**kwargs_results) 
          reader = results.ResultsReader(io.BufferedReader(ResponseReaderWrapper(result_stream)))
          for item in reader:
            yield item
            count = count + 1

          #If count still equals the previous offset, something went wrong.
          if count == kwargs_results['offset']:
            raise Exception("Bork")
 
dispatch(GenerateRemoteSplunkCommand, sys.argv, sys.stdin, sys.stdout, __name__)
