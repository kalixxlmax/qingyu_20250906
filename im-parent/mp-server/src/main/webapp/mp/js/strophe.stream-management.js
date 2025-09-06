/**
* StropheJS - Stream Management XEP-0198
*
* This plugin implements stream mangemament ACK capabilities of the specs XEP-0198.
* Note: Resumption is not supported in this current implementation.
*
* Reference: http://xmpp.org/extensions/xep-0198.html
*
* @class streamManagement
*/
Strophe.addConnectionPlugin('streamManagement', {

	/**
	* @property {Boolean} logging: Set to true to enable logging regarding out of sync stanzas.
	*/
	logging: true,

	/**
	* @property {Boolean} returnWholeStanza: Set to true to return the acknowledged stanzas, otherwise only return its ID.
	*/
	returnWholeStanza: false,

	/**
	* @property {Boolean} autoSendCountOnEveryIncomingStanza: Set to true to send an 'a' response after every stanza.
	* @default false
	* @public
	*/
	autoSendCountOnEveryIncomingStanza: false,

	/**
	* @property {Integer} requestResponseInterval: Set this value to send a request for counter on very interval
	* number of stanzas sent. Set to 0 to disable.
	* @default 5
	* @public
	*/
	requestResponseInterval: 5,

	/**
	* @property {Pointer} _c: Strophe connection instance.
	* @private
	*/
	_c: null,

	/**
	* @property {String} _NS XMPP Namespace.
	* @private
	*/
	_NS: 'urn:xmpp:sm:3',

	/**
	* @property {Boolean} _isStreamManagementEnabled
	* @private
	*/
	_isStreamManagementEnabled: false,

	/**
	* @property {Integer} _serverProcesssedStanzasCounter: Keeps count of stanzas confirmed processed by the server.
	* The server is the source of truth of this value. It is the 'h' attribute on the latest 'a' element received
	* from the server.
	* @private
	*/
	_serverProcesssedStanzasCounter: null,

	/**
	* @property {Integer} _clientProcessedStanzasCounter: Counter of stanzas received by the client from the server.
	* Client is the source of truth of this value. It is the 'h' attribute in the 'a' sent from the client to
	* the server.
	* @private
	*/
	_clientProcessedStanzasCounter: null,

	/**
	* @property {Integer} _clientSentStanzasCounter
	* @private
	*/
	_clientSentStanzasCounter: null,

	/**
	* Stores a reference to Strophe connection xmlOutput function to wrap counting functionality.
	* @method _originalXMLOutput
	* @type {Handler}
	* @private
	*/
	_originalXMLOutput: null,

	/**
	* @property {Handler} _requestHandler: Stores reference to handler that process count request from server.
	* @private
	*/
	_requestHandler: null,

	/**
	* @property {Handler} _incomingHandler: Stores reference to handler that processes incoming stanzas count.
	* @private
	*/
	_incomingHandler: null,

	/**
	* @property {Integer} _requestResponseIntervalCount: Counts sent stanzas since last response request.
	*/
	_requestResponseIntervalCount: 0,

	/**
	* @property {Queue} _unacknowledgedStanzas: Maintains a list of packet ids for stanzas which have yet to be acknowledged.
	*/
	_unacknowledgedStanzas: [],

	/**
	* @property {Array} _acknowledgedStanzaListeners: Stores callbacks for each stanza acknowledged by the server.
	* Provides the packet id of the stanza as a parameter.
	* @private
	*/
	_acknowledgedStanzaListener:null,

	addAcknowledgedStanzaListener(listener) {
		this._acknowledgedStanzaListener=listener;
	},
	_randomUUID : function() {
		var chars = myData.charArray, uuid = new Array(36), rnd = 0, r;
		for (var i = 0; i < 36; i++) {
			if (i == 8 || i == 13 || i == 18 || i == 23) {
				uuid[i] = '-';
			} else if (i == 14) {
				uuid[i] = '4';
			} else {
				if (rnd <= 0x02)
					rnd = 0x2000000 + (Math.random() * 0x1000000) | 0;
				r = rnd & 0xf;
				rnd = rnd >> 4;
				uuid[i] = chars[(i == 19) ? (r & 0x3) | 0x8 : r];
			}
		}
		return uuid.join('').replace(/-/gm, '').toLowerCase();
	},
	
	enable: function() {
		
		//resume:false,
		var stanza=$build('enable', {xmlns: this._NS,resume:true,to:AppConfig.boshDomain});
		SKIMSDK.selfSend(stanza,function(elem){
			SKIMSDK.connection.streamManagement.responseCall(elem);
		});
		/*this._c.flush();
		this._c.pause();*/
		
	},
	resumeStream:function(id){
		var stanza=$build('resume', {xmlns: this._NS,resume:true,previd:id,h:this._clientSentStanzasCounter,to:AppConfig.boshDomain});
		SKIMSDK.selfSend(stanza,function(elem){
			SKIMSDK.connection.streamManagement.responseCall(elem);
		});
	},
	responseCall:function(elem){
		this._incomingStanzaHandler(elem);
	},

	requestAcknowledgement: function() {

		this._requestResponseIntervalCount = 0;
		//this._c.send($build('r', { xmlns: this._NS }));
		var xmlns=this._NS;
		
		setTimeout(function(){
			var stanza=$build('r', { xmlns: xmlns,to:AppConfig.boshDomain});
			SKIMSDK.selfSend(stanza,function(elem){
				SKIMSDK.connection.streamManagement.responseCall(elem);
			});
		},2000);
		
	},

	getOutgoingCounter: function() {
		return this._clientSentStanzasCounter;
	},

	getIncomingCounter: function() {
		return this._clientProcessedStanzasCounter;
	},

	init: function(conn) {
		this._c = conn;
		Strophe.addNamespace('SM', this._NS);

		// Storing original xmlOutput function to use additional logic
		this._originalXMLOutput = this._c.xmlOutput;
		this._c.xmlOutput = this.xmlOutput.bind(this);
	},

	statusChanged: function (status) {
		if (status === Strophe.Status.CONNECTED || status === Strophe.Status.DISCONNECTED) {

			this._serverProcesssedStanzasCounter = 0;
			this._clientProcessedStanzasCounter = 0;

			this._clientSentStanzasCounter = 0;

			this._isStreamManagementEnabled = false;
			this._requestResponseIntervalCount = 0;

			this._unacknowledgedStanzas = [];

			if (this._requestHandler) {
				this._c.deleteHandler(this._requestHandler);
			}

			if (this._incomingHandler) {
				this._c.deleteHandler(this._incomingHandler);
			}

			this._requestHandler = this._c.addHandler(this._handleServerRequestHandler.bind(this), this._NS, 'r');
			this._incomingHandler = this._c.addHandler(this._incomingStanzaHandler.bind(this));
		}
	},

	/**
	* This method overrides the send method implemented by Strophe.Connection
	* to count outgoing stanzas
	*
	* @method Send
	* @public
	*/
	xmlOutput: function(elem) {
		//console.log("xmlOutput  "+Strophe.serialize(elem));
		var child;
		for (var i = 0; i < elem.children.length; i++) {
			child = elem.children[i];
			if(!child){
				return;
			}
			this._increaseSentStanzasCounter(child);
			/*if (Strophe.isTagEqual(child, 'iq') ||
			Strophe.isTagEqual(child, 'presence') ||
			Strophe.isTagEqual(child, 'message')) {
				
			}*/
		}

		return this._originalXMLOutput.call(this._c, elem);
	},

	_incomingStanzaHandler: function(elem) {
		
		if (Strophe.isTagEqual(elem, 'enabled') && elem.getAttribute('xmlns')==this._NS) {
			console.log("server========== enabled");
			this._isStreamManagementEnabled = true;
			var streamId=elem.getAttribute("id");
			SKIMSDK.connection.enabledStream(streamId);
			//this._c.resume();
		}

		if (Strophe.isTagEqual(elem, 'iq') || Strophe.isTagEqual(elem, 'presence') || Strophe.isTagEqual(elem, 'message'))  {
			this._increaseReceivedStanzasCounter();

			if (this.autoSendCountOnEveryIncomingStanza) {
				this._answerProcessedStanzas();
			}
		}

		if (Strophe.isTagEqual(elem, 'a')) {
			console.log("_incomingStanzaHandler=== "+Strophe.serialize(elem));
			var handledCount = parseInt(elem.getAttribute('h'));
			this._handleAcknowledgedStanzas(handledCount, this._serverProcesssedStanzasCounter);
			this._serverProcesssedStanzasCounter = handledCount;

			if (this.requestResponseInterval > 0) {
				this._requestResponseIntervalCount = 0;
			}
		}

		return true;
	},

	_handleAcknowledgedStanzas: function(reportedHandledCount, lastKnownHandledCount) {
		var delta = reportedHandledCount - lastKnownHandledCount;

		if (delta < 0) {
			this._throwError('New reported stanza count lower than previous. New: ' + reportedHandledCount + ' - Previous: ' + lastKnownHandledCount);
		}

		if (delta > this._unacknowledgedStanzas.length) {
			this._throwError('Higher reported acknowledge count than unacknowledged stanzas. Reported Acknowledge Count: ' + delta + ' - Unacknowledge Stanza Count: ' + this._unacknowledgedStanzas.length + ' - New: ' + reportedHandledCount + ' - Previous: ' + lastKnownHandledCount);
		}
		//var listener=this._acknowledgedStanzaListeners[0];
		/*if(!myFn.isNil(listener))
			return;*/
			
		for(var i = 0; i < delta; i++) {
			var stanza = this._unacknowledgedStanzas.shift();
			//listener = typeof listener === 'function'
			this._acknowledgedStanzaListener(stanza);
			/*for (var j = 0; j < this._acknowledgedStanzaListeners.length; j++) {
				this._acknowledgedStanzaListeners[j](stanza);
			}*/
		}

		if (this.logging && this._unacknowledgedStanzas.length > 0) {
			console.warn('Unacknowledged stanzas', this._unacknowledgedStanzas);
		}
	},

	_handleServerRequestHandler: function() {
		this._answerProcessedStanzas();
		return true;
	},

	_answerProcessedStanzas: function() {
		if (this._isStreamManagementEnabled) {
			var stanza= $build('a', { xmlns: this._NS, h: this._clientProcessedStanzasCounter,to:AppConfig.boshDomain });
			SKIMSDK.connection.selfSend(stanza,function(elem){
				SKIMSDK.connection.streamManagement.responseCall(elem);
			});
		}
	},

	_increaseSentStanzasCounter: function(elem) {
		if (this._isStreamManagementEnabled) {
			this._unacknowledgedStanzas.push(this.returnWholeStanza ? elem : elem.getAttribute('id'));

			this._clientSentStanzasCounter++;

			if (this.requestResponseInterval > 0) {
				this._requestResponseIntervalCount++;

				if (this._requestResponseIntervalCount==this.requestResponseInterval) {
					this.requestAcknowledgement();
				}
			}
		}
	},

	_increaseReceivedStanzasCounter: function() {
		if (this._isStreamManagementEnabled) {
			this._clientProcessedStanzasCounter++;
		}
	},

	_throwError(msg) {
		console.error(msg);
		throw new Error(msg);
	}

});