/*
ByteGenie classes for javascript
*/

const fetch = require('node-fetch'); // Use an appropriate HTTP library for your environment

class ByteGenieResponse {
  constructor(response) {
    if (typeof response !== 'object') {
      throw new Error('response must be an object');
    }
    this.response = response;
  }

  getStatus() {
    let status = 'scheduled';
    let resp = this.response;
    if (resp.response && typeof resp.response === 'object' && resp.response.task_1 && typeof resp.response.task_1 === 'object' && resp.response.task_1.status) {
      status = resp.response.task_1.status;
    }
    return status;
  }

  getData() {
    let resp = this.response;
    if (resp.response && typeof resp.response === 'object' && resp.response.task_1 && typeof resp.response.task_1 === 'object' && resp.response.task_1.data) {
      return resp.response.task_1.data;
    }
  }

  getOutputFile() {
    let resp = this.response;
    if (resp.response && typeof resp.response === 'object' && resp.response.task_1 && typeof resp.response.task_1 === 'object' && resp.response.task_1.task && typeof resp.response.task_1.task === 'object' && resp.response.task_1.task.output_file) {
      return resp.response.task_1.task.output_file;
    }
  }

  async checkOutputFileExists() {
    const bg = new ByteGenie({ task_mode: 'sync' });
    const output_file = this.getOutputFile();
    if (output_file) {
      const resp = await bg.checkFileExists(output_file);
      return resp.getData();
    } else {
      return false;
    }
  }

  async readOutputData() {
    const bg = new ByteGenie({ task_mode: 'sync' });
    if (await this.checkOutputFileExists()) {
      const resp = await bg.readFile(this.getOutputFile());
      return resp.getData();
    } else {
      console.warn("Output does not yet exist: wait some more");
    }
  }
}

class ByteGenie {
  constructor({
    api_url = 'https://api.esgnie.com/execute',
    secrets_file = 'secrets.json',
    task_mode = 'async',
    calc_mode = 'async',
    return_data = 1,
    overwrite = 0,
    overwrite_base_output = 0,
    verbose = 1,
  } = {}) {
    this.api_url = api_url;
    this.secrets_file = secrets_file;
    this.task_mode = task_mode;
    this.calc_mode = calc_mode;
    this.return_data = return_data;
    this.overwrite = overwrite;
    this.overwrite_base_output = overwrite_base_output;
    this.verbose = verbose;
    this.api_key = this.readApiKey();
  }

  readApiKey() {
    const fs = require('fs');
    const path = require('path');
    const filename = path.join(this.secrets_file);
    try {
      const secrets = JSON.parse(fs.readFileSync(filename, 'utf-8'));
      return secrets.BYTE_GENIE_KEY || '';
    } catch (error) {
      return '';
    }
  }

  async createApiPayload(func, args) {
    return {
      api_key: this.api_key,
      tasks: {
        task_1: {
          func: func,
          args: args,
          overwrite: this.overwrite,
          overwrite_base_output: this.overwrite_base_output,
          return_data: this.return_data,
          verbose: this.verbose,
          task_mode: this.task_mode,
          calc_mode: this.calc_mode,
        },
      },
    };
  }

  setHeaders() {
    return {
      Accept: '*/*',
      'Accept-Encoding': 'gzip, deflate',
      Authorization: 'Basic ZTgxMDg5NGY4NWNkNmU5ODc1ZDNiZjY1ODc0ZmExYjk6YjY4YmQ5ZTgwMTgxMzJiZGEyODNhZmZmOWFlNDY5NzU=',
      Connection: 'keep-alive',
      'Content-Type': 'application/json',
    };
  }

  async callApi(payload, method = 'POST', timeout = 15 * 60) {
    const headers = this.setHeaders();
    const response = await fetch(this.api_url, {
      method: method,
      headers: headers,
      body: JSON.stringify(payload),
      timeout: timeout,
    });
    try {
      const jsonResp = await response.json();
      return new ByteGenieResponse(jsonResp);
    } catch (e) {
      const jsonResp = { payload: payload, error: e };
      return new ByteGenieResponse(jsonResp);
    }
  }

  async slugify(text, timeout = 15 * 60) {
    const func = 'slugify';
    const args = { text: text };
    const payload = await this.createApiPayload(func, args);
    const resp = await this.callApi(payload, timeout);
    return resp;
  }

  async uploadData(contents, filenames, username, timeout = 15 * 60) {
    const func = 'upload_data';
    const args = { contents: contents, filenames: filenames, username: username };
    const payload = await this.createApiPayload(func, args);
    const resp = await this.callApi(payload, timeout);
    return resp;
  }

  async listDocFiles(doc_name, file_pattern, timeout = 15 * 60) {
    const func = 'list_doc_files';
    const args = { doc_name: doc_name, file_pattern: file_pattern };
    const payload = await this.createApiPayload(func, args);
    const resp = await this.callApi(payload, timeout);
    return resp;
  }
}

// Usage example:
// const bg = new ByteGenie();
// bg.slugify('example text').then(response => {
//   console.log(response.getData());
// });
