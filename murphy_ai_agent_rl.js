/**
 * ============================================================
 *  MURPHY INDUSTRIAL PRODUCTS - AI AGENT RESTLET
 * ============================================================
 *  File:       murphy_ai_agent_rl.js
 *  Type:       RESTlet (SuiteScript 2.1)
 *  Version:    1.1.0
 *  Updated:    2026-04-07
 *  Author:     Murphy Industrial / Claude AI
 *
 *  Description:
 *    Intelligence layer for the Murphy AI Agent chat interface.
 *    Claude has two tools:
 *      1. run_suiteql      - Live NetSuite data queries
 *      2. track_fedex      - FedEx tracking + POD lookup
 *
 *    FedEx flow:
 *      a) POST /oauth/token -> access token
 *      b) POST /track/v1/trackingnumbers -> delivery status + POD
 *
 *    The agent can look up a sales order, find the tracking
 *    number on the item fulfillment, then call FedEx to return
 *    full delivery confirmation including signed-by name.
 *
 *  Script ID:    customscript_murphy_ai_agent_rl
 *  Deploy ID:    customdeploy_murphy_ai_agent_rl
 *
 *  Script Parameters:
 *    custscript_agent_api_key  Text  Anthropic API key
 *
 *  Changelog:
 *    1.0.0 - Initial release
 *    1.0.1 - Fixed: removed all template literals for NS compatibility
 *    1.1.0 - Added: track_fedex tool for POD lookups via FedEx Track API
 * ============================================================
 *
 * @NApiVersion 2.1
 * @NScriptType Restlet
 * @NModuleScope SameAccount
 */
define(['N/https', 'N/query', 'N/runtime', 'N/log'],
function (https, query, runtime, log) {

  var CLAUDE_MODEL = 'claude-opus-4-6';
  var MAX_LOOPS    = 6;

  // ─────────────────────────────────────────────────────────────
  //  FEDEX CREDENTIALS
  // ─────────────────────────────────────────────────────────────
  var FEDEX_API_KEY    = 'l7d759e83e4e034170b88c1b6d389989eb';
  var FEDEX_API_SECRET = '974d8dd82afe43a7a0a379e2daadb35c';
  var FEDEX_ACCT       = '318723273';
  var FEDEX_BASE_URL   = 'https://apis.fedex.com';

  // ─────────────────────────────────────────────────────────────
  //  SYSTEM PROMPT
  // ─────────────────────────────────────────────────────────────
  var SYSTEM_PROMPT =
    'You are the Murphy Industrial Products AI Agent - a knowledgeable, helpful assistant ' +
    'built into Murphy\'s NetSuite ERP system. You work alongside the Murphy team every day.\n\n' +

    'Murphy Industrial Products is based in Houston, TX. They sell industrial supplies, ' +
    'rigging, lifting equipment, wire rope, chain, hooks, hoists, and safety products. ' +
    'Customers include manufacturers, equipment rental companies, construction firms, ' +
    'oil & gas companies, and industrial distributors.\n\n' +

    '## YOUR TOOLS\n\n' +

    '### run_suiteql\n' +
    'Query live NetSuite data. Use for inventory, customers, vendors, orders, pricing, contacts.\n' +
    'Always query real data before answering questions about:\n' +
    '- Stock levels, inventory, reorder points\n' +
    '- Customer or prospect records\n' +
    '- Vendor information or purchase history\n' +
    '- Sales orders or order history\n' +
    '- Item fulfillments and tracking numbers\n' +
    '- Pricing or cost information\n\n' +

    '### track_fedex\n' +
    'Look up FedEx tracking status and Proof of Delivery for any tracking number.\n' +
    'Use when a user asks about:\n' +
    '- Whether a shipment was delivered\n' +
    '- POD / proof of delivery\n' +
    '- Who signed for a package\n' +
    '- Delivery date and time\n' +
    '- Current tracking status of an in-transit shipment\n\n' +
    'WORKFLOW for POD requests:\n' +
    '1. Query NetSuite for the sales order or item fulfillment to get the tracking number\n' +
    '2. Call track_fedex with that tracking number\n' +
    '3. Return the delivery confirmation details to the user\n\n' +

    '## MURPHY NETSUITE STRUCTURE\n' +
    'Account: 8156948 | Subsidiary: 2 (Murphy Industrial Products, Inc.) | Location: 1\n\n' +

    'Key tables:\n' +
    '- customer: id, companyname, entitystatus, email, phone, parent\n' +
    '  entitystatus: 6=Lead-Unqualified, 7=Lead-Qualified, 10=Prospect-Proposal,\n' +
    '  12=Prospect-Purchasing, 13=Customer-Closed Won\n' +
    '- contact: id, firstname, lastname, email, phone, title, company\n' +
    '- vendor: id, companyname, email, phone\n' +
    '  vendor plan fields: custentity_mip_monthly_plan, custentity_mip_plan_day, custentity_mip_lead_days\n' +
    '- item: id, itemid (SKU), displayname, averagecost, itemtype\n' +
    '- transaction: id, tranid, type, trandate, status, entity, trackingnumbers\n' +
    '  types: PurchOrd, SalesOrd, ItemShip (fulfillment), CustInvc, VendBill\n' +
    '  ItemShip is the fulfillment record - has trackingnumbers field\n' +
    '- transactionline: transaction, item, quantity, rate, amount\n' +
    '- itemvendor: item, vendor, preferredvendor (T/F)\n\n' +

    '## SUITEQL RULES\n' +
    '- AND isinactive = \'F\' on customer/item/vendor\n' +
    '- UPPER() for case-insensitive searches\n' +
    '- ROWNUM for limits\n' +
    '- DO NOT join itemlocation - it causes errors. For inventory use item averagecost only\n' +
    '- Transaction type for item fulfillments: ItemShip\n' +
    '- trackingnumbers field is on the transaction table for ItemShip records\n\n' +

    '## MURPHY DEFAULTS\n' +
    'Subsidiary: 2 | Form: 142 | Sales Rep: Tim Murphy (-5)\n' +
    'Status: Prospect-Proposal (10) | Terms: Credit Card (8)\n' +
    'Price Level: 50% M.U. (6) | Industry: Industrial Supply (43)\n\n' +

    '## HOW TO RESPOND\n' +
    '- Be conversational and direct - like a Murphy coworker who knows the system\n' +
    '- Use "we", "our", "us" naturally\n' +
    '- Always query real data before stating facts\n' +
    '- Format numbers clearly ($1,234.56)\n' +
    '- Flag problems proactively\n' +
    '- Keep responses concise unless detail is needed';

  // ─────────────────────────────────────────────────────────────
  //  TOOL DEFINITIONS
  // ─────────────────────────────────────────────────────────────
  var TOOLS = [
    {
      name        : 'run_suiteql',
      description : 'Run a SuiteQL query against Murphy Industrial\'s live NetSuite data. ' +
                    'Use to look up inventory, customers, vendors, orders, tracking numbers, or ' +
                    'any other NetSuite data needed to answer the question accurately.',
      input_schema: {
        type      : 'object',
        properties: {
          query      : { type: 'string', description: 'The SuiteQL query to execute' },
          description: { type: 'string', description: 'What this query retrieves' },
        },
        required: ['query', 'description'],
      },
    },
    {
      name        : 'track_fedex',
      description : 'Look up FedEx tracking status and Proof of Delivery for a shipment. ' +
                    'Returns delivery date, time, city, signed-by name if available, and full event history. ' +
                    'Use after getting the tracking number from NetSuite item fulfillment.',
      input_schema: {
        type      : 'object',
        properties: {
          tracking_number: {
            type       : 'string',
            description: 'The FedEx tracking number from the item fulfillment record',
          },
        },
        required: ['tracking_number'],
      },
    },
  ];

  // ═══════════════════════════════════════════════════════════════
  //  ENTRY POINT
  // ═══════════════════════════════════════════════════════════════
  function post(body) {
    try {
      var messages = body.messages || [];
      var userMsg  = body.message  || '';

      if (!userMsg && !messages.length) { return { error: 'No message provided.' }; }
      if (userMsg) { messages.push({ role: 'user', content: userMsg }); }

      var apiKey = getApiKey();
      if (!apiKey) { return { error: 'Anthropic API key not configured on script parameters.' }; }

      var loopCount  = 0;
      var finalReply = null;

      while (loopCount < MAX_LOOPS) {
        loopCount++;

        var claudeResp = callClaude(apiKey, messages);
        if (claudeResp.error) { return { error: claudeResp.error }; }

        var content    = claudeResp.content || [];
        var stopReason = claudeResp.stop_reason;

        messages.push({ role: 'assistant', content: content });

        if (stopReason === 'end_turn') {
          finalReply = content
            .filter(function(b) { return b.type === 'text'; })
            .map(function(b) { return b.text; })
            .join('');
          break;
        }

        if (stopReason === 'tool_use') {
          var toolUseBlocks = content.filter(function(b) { return b.type === 'tool_use'; });
          var toolResults   = [];

          toolUseBlocks.forEach(function(toolUse) {
            var result;

            if (toolUse.name === 'run_suiteql') {
              var sql  = (toolUse.input && toolUse.input.query) || '';
              var desc = (toolUse.input && toolUse.input.description) || '';
              result = executeSuiteQL(sql, desc);
              log.audit({
                title  : 'Murphy AI Agent: SuiteQL',
                details: desc + ' | Rows: ' + result.rowCount
              });

            } else if (toolUse.name === 'track_fedex') {
              var trackNum = (toolUse.input && toolUse.input.tracking_number) || '';
              result = trackFedEx(trackNum);
              log.audit({
                title  : 'Murphy AI Agent: FedEx Track',
                details: 'Tracking: ' + trackNum + ' | Success: ' + result.success
              });

            } else {
              result = { error: 'Unknown tool: ' + toolUse.name };
            }

            toolResults.push({
              type       : 'tool_result',
              tool_use_id: toolUse.id,
              content    : JSON.stringify(result),
            });
          });

          messages.push({ role: 'user', content: toolResults });
          continue;
        }

        break;
      }

      if (!finalReply) {
        finalReply = 'I wasn\'t able to complete that request. Please try rephrasing your question.';
      }

      return { reply: finalReply, messages: messages };

    } catch (e) {
      log.error({ title: 'Murphy AI Agent: fatal error', details: e.toString() });
      return { error: 'Agent error: ' + e.message };
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  CLAUDE API
  // ═══════════════════════════════════════════════════════════════
  function callClaude(apiKey, messages) {
    try {
      var resp = https.post({
        url    : 'https://api.anthropic.com/v1/messages',
        headers: {
          'Content-Type'     : 'application/json',
          'x-api-key'        : apiKey,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model      : CLAUDE_MODEL,
          max_tokens : 2048,
          system     : SYSTEM_PROMPT,
          tools      : TOOLS,
          messages   : messages,
        }),
      });

      if (resp.code !== 200) {
        log.error({ title: 'Murphy AI Agent: Claude error', details: resp.body });
        return { error: 'Claude API error: ' + resp.code };
      }

      return JSON.parse(resp.body);

    } catch (e) {
      log.error({ title: 'Murphy AI Agent: Claude call failed', details: e.toString() });
      return { error: 'Failed to reach Claude API: ' + e.message };
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  SUITEQL EXECUTION
  // ═══════════════════════════════════════════════════════════════
  function executeSuiteQL(sql, description) {
    try {
      var results = query.runSuiteQL({ query: sql }).asMappedResults();
      return {
        success    : true,
        description: description,
        rowCount   : results.length,
        rows       : results.slice(0, 50),
        truncated  : results.length > 50,
      };
    } catch (e) {
      log.error({ title: 'Murphy AI Agent: SuiteQL failed', details: sql + '\n' + e.toString() });
      return { success: false, error: e.message, query: sql };
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  FEDEX TRACKING
  // ═══════════════════════════════════════════════════════════════

  // Step 1 - Get OAuth token
  function getFedExToken() {
    try {
      var resp = https.post({
        url    : FEDEX_BASE_URL + '/oauth/token',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body   : 'grant_type=client_credentials' +
                 '&client_id=' + FEDEX_API_KEY +
                 '&client_secret=' + FEDEX_API_SECRET,
      });

      if (resp.code !== 200) {
        log.error({ title: 'FedEx token error', details: resp.body });
        return null;
      }

      var data = JSON.parse(resp.body);
      return data.access_token || null;

    } catch (e) {
      log.error({ title: 'FedEx token exception', details: e.toString() });
      return null;
    }
  }

  // Step 2 - Track shipment
  function trackFedEx(trackingNumber) {
    if (!trackingNumber) {
      return { success: false, error: 'No tracking number provided.' };
    }

    var token = getFedExToken();
    if (!token) {
      return { success: false, error: 'Could not authenticate with FedEx API.' };
    }

    try {
      var body = JSON.stringify({
        includeDetailedScans: true,
        trackingInfo: [{
          trackingNumberInfo: {
            trackingNumber    : trackingNumber,
            carrierCode       : 'FDXG',
            trackingNumberUniqueId: '',
          },
        }],
      });

      var resp = https.post({
        url    : FEDEX_BASE_URL + '/track/v1/trackingnumbers',
        headers: {
          'Content-Type'  : 'application/json',
          'Authorization' : 'Bearer ' + token,
          'x-customer-transaction-id': 'murphy-ai-agent',
          'x-locale'      : 'en_US',
        },
        body: body,
      });

      if (resp.code !== 200) {
        log.error({ title: 'FedEx track error', details: 'HTTP ' + resp.code + ' | ' + resp.body });
        return { success: false, error: 'FedEx API error: HTTP ' + resp.code, rawResponse: resp.body };
      }

      var data = JSON.parse(resp.body);
      return parseFedExResponse(data, trackingNumber);

    } catch (e) {
      log.error({ title: 'FedEx track exception', details: e.toString() });
      return { success: false, error: 'FedEx tracking error: ' + e.message };
    }
  }

  // Step 3 - Parse FedEx response into clean result
  function parseFedExResponse(data, trackingNumber) {
    try {
      var output = data.output || {};
      var results = (output.completeTrackResults || [])[0] || {};
      var trackResults = (results.trackResults || [])[0] || {};

      // Delivery details
      var dateTime       = trackResults.estimatedDeliveryTimeWindow
                        || trackResults.actualDeliveryTime
                        || '';
      var deliveryDetail = trackResults.deliveryDetails || {};
      var deliveryDate   = deliveryDetail.actualDeliveryAddress
                        ? null
                        : (trackResults.actualDeliveryTime || '');
      var deliveryLoc    = deliveryDetail.actualDeliveryAddress || {};
      var signedBy       = deliveryDetail.deliverySignatoryName || deliveryDetail.signedByName || '';
      var deliveryAttempts = deliveryDetail.deliveryAttempts || '';

      // Status
      var statusDetail   = trackResults.latestStatusDetail || {};
      var statusCode     = statusDetail.code || '';
      var statusDesc     = statusDetail.description || statusDetail.statusByLocale || '';
      var statusDate     = statusDetail.scanLocation
                        ? (statusDetail.scanLocation.city || '') + ', ' + (statusDetail.scanLocation.stateOrProvinceCode || '')
                        : '';

      // Scan events for full history
      var scanEvents = (trackResults.scanEvents || []).slice(0, 10).map(function(e) {
        var loc = e.scanLocation || {};
        return {
          date       : e.date || '',
          eventType  : e.eventType || '',
          description: e.eventDescription || '',
          location   : (loc.city || '') + (loc.stateOrProvinceCode ? ', ' + loc.stateOrProvinceCode : ''),
        };
      });

      var delivered = statusCode === 'DL' || (statusDesc && statusDesc.toLowerCase().indexOf('delivered') !== -1);

      return {
        success        : true,
        trackingNumber : trackingNumber,
        delivered      : delivered,
        status         : statusDesc || statusCode,
        statusLocation : statusDate,
        deliveredDate  : trackResults.actualDeliveryTime || '',
        signedBy       : signedBy || (delivered ? 'Signature not required or not captured' : ''),
        deliveryCity   : (deliveryLoc.city || '') + (deliveryLoc.stateOrProvinceCode ? ', ' + deliveryLoc.stateOrProvinceCode : ''),
        estimatedDelivery: trackResults.estimatedDeliveryTimeWindow || '',
        serviceType    : trackResults.serviceDetail ? (trackResults.serviceDetail.description || '') : '',
        weight         : trackResults.packageDetails ? (trackResults.packageDetails.weightAndDimensions || '') : '',
        scanEvents     : scanEvents,
      };

    } catch (e) {
      log.error({ title: 'FedEx parse error', details: e.toString() });
      return {
        success        : true,
        trackingNumber : trackingNumber,
        rawData        : data,
        parseError     : e.message,
      };
    }
  }

  // ─────────────────────────────────────────────────────────────
  //  HELPERS
  // ─────────────────────────────────────────────────────────────
  function getApiKey() {
    try {
      return runtime.getCurrentScript().getParameter({ name: 'custscript_agent_api_key' }) || '';
    } catch (e) { return ''; }
  }

  return { post: post };
});
