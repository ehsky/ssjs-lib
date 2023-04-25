/**
 * @copyright   [email360](https://www.email360.io/)
 * @author      [Erlend Hansen](https://trailblazer.me/id/ehansen7)
 * @since       2023
 *
 * Collection of wrapper functions for the usage of the Salesforce Core REST API.
 *
 * @param {object} [settings] Used to update the settings object with a custom setting object
 *
 * @example
 * // initialise a new sfcoreapi instance
 * var rest = new sfcoreapi();
 */

//TODO: implement custom settings parameter
function sfcoreapi(settings) {
  // this.settings = _updateSettings(settings);

  // base URIs
  // TODO: Implement

  /* ============================================================================
                                TOKEN MANAGEMENT
    ============================================================================ */

  function getSFCredentials(mid) {
    if (mid) {
      prox.setClientId({ ID: mid }); //Impersonates the BU
    }

    //External key of DE with SF Credentials
    var deCustKey = "XXXX-XXXX-XXXXX-XXXXXX";
    var cols = [
      "ConnectionName",
      "ClientId",
      "ClientSecret",
      "Username",
      "Password",
    ];
    var deReturn = prox.retrieve(
      "DataExtensionObject[" + deCustKey + "]",
      cols
    );
    var deResults = deReturn.Results;
    var credRtvd = false;

    for (var i = 0; i < deResults.length; i++) {
      if (credRtvd) break;
      var deRecord = deResults[i];
      for (var j = 0; j < deRecord.Properties.length; j++) {
        var name = deRecord.Properties[j].Name;
        var value = deRecord.Properties[j].Value;

        if (name == "ConnectionName" && value == "SF CRM Connected App") {
          credRtvd = true;
        }
        if (name == "ClientId") {
          clientid = value;
        }
        if (name == "ClientSecret") {
          clientsecret = value;
        }
        if (name == "Username") {
          username = value;
        }
        if (name == "Password") {
          password = value;
        }
      }
    }
  }

  function retrieveToken(host, clientid, clientsecret, username, password) {
    var authPath =
      "/oauth2/token?grant_type=password&client_id=" +
      clientid +
      "&client_secret=" +
      clientsecret +
      "&username=" +
      username +
      "&password=" +
      password;
    var url = host + authPath;
    var req = new Script.Util.HttpRequest(url);
    req.emptyContentHandling = 0;
    req.retries = 2;
    req.continueOnError = true;
    req.contentType = "application/json";
    req.method = "POST";

    var res = req.send();
    var resultStr = String(res.content);
    var resultJSON = Platform.Function.ParseJSON(String(res.content));

    return resultJSON;
  }

  /* ============================================================================
                                        BULK v2 
    ============================================================================ */

  function createBulkReq(host, token) {
    var url = host + "/services/data/v49.0/jobs/ingest/";
    var payload = {};
    payload.object = "Subscriber__c";
    payload.contentType = "CSV";
    payload.operation = "insert";
    payload.lineEnding = "CRLF";

    var req = new Script.Util.HttpRequest(url);
    req.emptyContentHandling = 0;
    req.retries = 2;
    req.continueOnError = true;
    req.contentType = "application/json";
    req.method = "POST";
    req.setHeader("Authorization", "Bearer " + token);
    req.postData = Stringify(payload);

    var res = req.send();
    var resultStr = String(res.content);
    var resultJSON = Platform.Function.ParseJSON(String(res.content));

    return resultJSON;
  }

  function processBatchData(instURL, token, jobid, mid) {
    //Provide External key for SFSubscribersDE
    var deKey = "XXXX-XXXX-XXXX-XXXX";
    //SFSubscribersDE Fields
    var columns = [
      "CustomerId",
      "FirstName",
      "LastName",
      "Email",
      "Gender",
      "Title",
      "Status",
    ];
    var moreData = true; //To validate if more data in Retrieve
    var reqID = null; //Used with Batch Retrieve to get more data
    var batchCount = 0;
    //String to store CSV Data to send to SF Bulk API
    var csvData =
      "Customer_ID__c,First_Name__c,Last_Name__c,Email__c,Gender__c,Title__c,Status__c" +
      "\r\n";

    while (moreData) {
      batchCount++;
      moreData = false;
      //Call function to get records from DE
      var deReturn = getDERowsArray(mid, deKey, columns, reqID);

      moreData = deReturn.HasMoreRows;
      reqID = deReturn.RequestID;

      //iterate for each batch of 2500 records returned
      for (var i = 0; i < deReturn.Results.length; i++) {
        var recArray = [];
        var currRecord = deReturn.Results[i];
        for (var j = 0; j < currRecord.Properties.length; j++) {
          if (currRecord.Properties[j].Name != "_CustomObjectKey")
            recArray.push(currRecord.Properties[j].Value);
        }
        csvData += recArray.join(",") + "\r\n";
      }
      //Use batchCount if needed for debug log to identify number of batches called;
    }
    //Send update request to Bulk API job with final CSV Data
    var updJobJSON = updateBulkReq(instURL, token, jobid, csvData);
  }

  function getDERowsArray(mid, deCustKey, cols, reqID) {
    if (mid) {
      prox.setClientId({ ID: mid }); //Impersonates the BU
    }

    if (reqID == null) {
      var deRecs = prox.retrieve(
        "DataExtensionObject[" + deCustKey + "]",
        cols
      ); //executes the proxy call
    } else {
      deRecs = prox.getNextBatch(
        "DataExtensionObject[" + deCustKey + "]",
        reqID
      );
    }

    return deRecs;
  }

  function updateBulkReq(host, token, jobid, csvData) {
    var url = host + "/services/data/v49.0/jobs/ingest/" + jobid + "/batches";
    var req = new Script.Util.HttpRequest(url);
    req.emptyContentHandling = 0;
    req.retries = 2;
    req.continueOnError = true;
    req.contentType = "text/csv";
    req.method = "PUT";
    req.setHeader("Authorization", "Bearer " + token);
    req.postData = csvData;

    var resp = req.send();
    var resultStr = String(resp.content);
    var resultJSON = Platform.Function.ParseJSON(String(resp.content));

    return resultJSON;
  }

  function closeBulkReq(host, token, jobid) {
    var url = host + "/services/data/v49.0/jobs/ingest/" + jobid;
    var payload = {};
    payload.state = "UploadComplete";

    var req = new Script.Util.HttpRequest(url);
    req.emptyContentHandling = 0;
    req.retries = 2;
    req.continueOnError = true;
    req.contentType = "application/json";
    req.method = "PATCH";
    req.setHeader("Authorization", "Bearer " + token);
    req.postData = Stringify(payload);

    var resp = req.send();
    var resultStr = String(resp.content);
    var resultJSON = Platform.Function.ParseJSON(String(resp.content));

    return resultJSON;
  }
}
