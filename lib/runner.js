/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

'use strict';

const EventEmitter = require('events').EventEmitter;
const path = require('path');
const _ = require('lodash');
const debug = require('debug')('runner');
const debugPerf = require('debug')('perf');
const uuid = require('uuid');
const A = require('async');
const Stats = require('./stats2');
const JSCK = require('jsck');
const tryResolve = require('try-require').resolve;
const createPhaser = require('./phases');
const createReader = require('./readers');
const engineUtil = require('./engine_util');
const wl = require('./weighted-pick');
const fs = require('fs')
const l = require('lodash');
const moment = require('moment');

const Engines = {
    http: {},
    ws: {},
    socketio: {}
};

JSCK.Draft4 = JSCK.draft4;

const schema = new JSCK.Draft4(require('./schemas/artillery_test_script.json'));

module.exports = {
    runner: runner,
    validate: validate,
    stats: Stats
};


let accountId = '537237850156';
let crossAccountRole = 'X-Account-Reporting-Role';
let stsParams = { RoleArn: '', RoleSessionName: 'Reporting-DevOps-Role' };
let resourceParams = { region: '', accessKeyId: '', secretAccessKey: '', sessionToken: '', accountId: '' };
const setCreds = (stsParams) => {
    const sts = new aws.STS();
    let accountId = stsParams.RoleArn.match(/\d{12}/g).join("");
    return new Promise((resolve, reject) => {
            console.info("stsParams received: ", JSON.stringify(stsParams));
            // if (stsParams.RoleArn === context.invokedFunctionArn) {
            //     resolve({ data: "self", accountId: accountId });
            // } else {
            sts.assumeRole(stsParams, (err, data) => {
                if (err) throw ("assume role error: ", err);
                else resolve({ data: data.Credentials, accountId: accountId });
            });
            //}
        })
        .then(creds => {
            if (creds.data === "self") {
                resourceParams.accountId = creds.accountId;
                resourceParams.region = 'us-west-2';
                return resourceParams;
            } else {
                resourceParams.accessKeyId = creds.data.AccessKeyId;
                resourceParams.secretAccessKey = creds.data.SecretAccessKey;
                resourceParams.sessionToken = creds.data.SessionToken;
                resourceParams.accountId = creds.accountId;
                resourceParams.region = 'us-west-2';
                return resourceParams;
            }
        });
};


const processEvent = (resourceParams, html_report) => {

    return new Promise((resolve, reject) => {
        var datetime = new Date();
        var d = datetime.toString();
        const db = new aws.DynamoDB(resourceParams);
        var appname = process.env.APP.split("--")[1];

        var params = {
            ExpressionAttributeNames: {
                "#ART": "ARTILLERY_S3KEY",
                "#DT": "LAST_UPDATE"

            },
            ExpressionAttributeValues: {
                ":art": {
                    S: appname + '/' + process.env.STAGE + '/artillery/' + html_report // 'artillery-report.html'
                },
                ":dt": {
                    S: d
                }

            },
            Key: {
                "APP": {
                    S: appname
                },
                "STAGE": {
                    S: process.env.STAGE
                }
            },
            ReturnValues: "ALL_NEW",
            TableName: "QA-REPORTS",
            UpdateExpression: "SET #ART = :art, #DT = :dt"
        };

        db.updateItem(params, function(err, data) {
            if (err) console.log(err, err.stack); // an error occurred
            else console.log(data); // successful response
            /*
            data = {
             ConsumedCapacity: {
              CapacityUnits: 1, 
              TableName: "Music"
             }
            }
            */
        });


        var s3 = new aws.S3(resourceParams);
        // call S3 to retrieve upload file to specified bucket
        var uploadParams = {
            Bucket: process.env.S3BUCKET,
            Key: '',
            Body: '',
            ACL: 'bucket-owner-full-control',
            ContentType: 'text/html'
        };
        var file = '/tmp/' + html_report; //'/tmp/artillery-report.html';


        var fileStream = fs.createReadStream(file);
        fileStream.on('error', function(err) {
            console.log('File Error', err);
        });
        uploadParams.Body = fileStream;


        uploadParams.Key = appname + '/' + process.env.STAGE + '/artillery/' + html_report; //'artillery-report.html'; //process.env.S3KEY + '/artillery/' + 'artillery-report.html';

        // call S3 to retrieve upload file to specified bucket
        s3.upload(uploadParams, function(err, data) {
            if (err) {
                console.log("Error", err);
            }
            if (data) {
                console.log("Upload Success", data.Location);
            }
        });


        var cwevents = new aws.CloudWatchEvents(resourceParams);


        var cweparams = {
            Entries: [{
                Detail: '{ \"appName\": \"' + appname + '\", \"s3key\": \"' + uploadParams.Key + '\", \"event\": \"' + appname + '-loadtest-complete' + '\", \"Region\": \"us-west-2\", \"Env\": \"' + process.env.STAGE + '\" }',
                DetailType: 'artilleryStatus',
                Source: 'com.nuskin.artillery'
            }]
        };

        resolve(cwevents.putEvents(cweparams, function(err, data) {
            if (err) {
                console.log("Error", err);
            } else {
                console.log("Success", data.Entries);
            }
        }));

    });

};


function validate(script) {
    let validation = schema.validate(script);
    return validation;
}

function runner(script, payload, options, callback) {
    let opts = _.assign({
            periodicStats: script.config.statsInterval || 10,
            mode: script.config.mode || 'uniform'
        },
        options);

    let warnings = {
        plugins: {
            // someplugin: {
            //   message: 'Plugin not found',
            //   error: new Error()
            // }
        },
        engines: {
            // see plugins
        }
    };

    _.each(script.config.phases, function(phaseSpec) {
        phaseSpec.mode = phaseSpec.mode || script.config.mode;
    });

    if (payload) {
        if (_.isArray(payload[0])) {
            script.config.payload = [{
                fields: script.config.payload.fields,
                reader: createReader(script.config.payload.order),
                data: payload
            }];
        } else {
            script.config.payload = payload;
            _.each(script.config.payload, function(el) {
                el.reader = createReader(el.order);
            });
        }
    } else {
        script.config.payload = null;
    }

    let runnableScript = _.cloneDeep(script);

    // Flatten flows (can have nested arrays of request specs with YAML references):
    _.each(runnableScript.scenarios, function(scenarioSpec) {
        scenarioSpec.flow = _.flatten(scenarioSpec.flow);
    });

    let ee = new EventEmitter();

    //
    // load engines:
    //
    let runnerEngines = _.map(
        Object.assign({}, Engines, runnableScript.config.engines),
        function loadEngine(engineConfig, engineName) {
            let moduleName = 'artillery-engine-' + engineName;
            try {
                if (Engines[engineName]) {
                    moduleName = './engine_' + engineName;
                }
                let Engine = require(moduleName);
                let engine = new Engine(runnableScript, ee, engineUtil);
                engine.__name = engineName;
                return engine;
            } catch (err) {
                console.log(
                    'WARNING: engine %s specified but module %s could not be loaded',
                    engineName,
                    moduleName);
                console.log(err.stack);
                warnings.engines[engineName] = {
                    message: 'Could not load',
                    error: err
                };
            }
        }
    );

    //
    // load plugins:
    //
    let runnerPlugins = [];
    let requirePaths = [];

    let pro = null;
    if (tryResolve('artillery-pro')) {
        pro = require('artillery-pro');
        requirePaths = requirePaths.concat(pro.getPluginPath());
    } else {
        debug('Artillery Pro is not installed.');
    }

    requirePaths.push('');

    if (process.env.ARTILLERY_PLUGIN_PATH) {
        requirePaths = requirePaths.concat(process.env.ARTILLERY_PLUGIN_PATH.split(':'));
    }

    debug('require paths: ', requirePaths);

    runnableScript.config.plugins = runnableScript.config.plugins || {};

    if (process.env.ARTILLERY_PLUGINS) {
        let additionalPlugins = {};
        try {
            additionalPlugins = JSON.parse(process.env.ARTILLERY_PLUGINS);
        } catch (ignoreErr) {
            debug(ignoreErr);
        }
        runnableScript.config.plugins = Object.assign(
            runnableScript.config.plugins,
            additionalPlugins);
    }

    _.each(runnableScript.config.plugins, function tryToLoadPlugin(pluginConfig, pluginName) {
        let pluginConfigScope = pluginConfig.scope || runnableScript.config.pluginsScope;
        let pluginPrefix = pluginConfigScope ? pluginConfigScope : 'artillery-plugin-';
        let requireString = pluginPrefix + pluginName;
        let Plugin, plugin;

        requirePaths.forEach(function(rp) {
            try {
                Plugin = require(path.join(rp, requireString));
                if (typeof Plugin === 'function') {
                    // Plugin interface v1
                    plugin = new Plugin(runnableScript.config, ee);
                    plugin.__name = pluginName;
                } else if (typeof Plugin === 'object' && typeof Plugin.Plugin === 'function') {
                    // Plugin interface 2+
                    plugin = new Plugin.Plugin(runnableScript, ee, options);
                    plugin.__name = pluginName;
                }
            } catch (err) {
                debug(err);
            }
        });

        if (!Plugin || !plugin) {
            console.log(
                'WARNING: plugin %s specified but module %s could not be loaded',
                pluginName,
                requireString);
            warnings.plugins[pluginName] = {
                message: 'Could not load'
            };
        } else {
            debug('Plugin %s loaded from %s', pluginName, requireString);
            runnerPlugins.push(plugin);
        }
    });

    const promise = new Promise(function(resolve, reject) {
        ee.run = function() {
            let runState = {
                pendingScenarios: 0,
                pendingRequests: 0,
                compiledScenarios: null,
                scenarioEvents: null,
                picker: undefined,
                plugins: runnerPlugins,
                engines: runnerEngines
            };
            debug('run() with: %j', runnableScript);
            run(runnableScript, ee, opts, runState);
        };

        ee.stop = function(done) {
            // allow plugins to cleanup
            A.eachSeries(
                runnerPlugins,
                function(plugin, next) {
                    if (plugin.cleanup) {
                        plugin.cleanup(function(err) {
                            if (err) {
                                debug(err);
                            }
                            return next();
                        });
                    } else {
                        return next();
                    }
                },
                function(err) {
                    return done(err);
                });
        };

        // FIXME: Warnings should be returned from this function instead along with
        // the event emitter. That will be a breaking change.
        ee.warnings = warnings;

        resolve(ee);
    });

    if (callback && typeof callback === 'function') {
        promise.then(callback.bind(null, null), callback);
    }

    return promise;
}

function run(script, ee, options, runState) {
    let intermediate = Stats.create();
    let aggregate = [];

    let phaser = createPhaser(script.config.phases);
    phaser.on('arrival', function() {
        runScenario(script, intermediate, runState);
    });
    phaser.on('phaseStarted', function(spec) {
        ee.emit('phaseStarted', spec);
    });
    phaser.on('phaseCompleted', function(spec) {
        ee.emit('phaseCompleted', spec);
    });
    phaser.on('done', function() {
        debug('All phases launched');

        const doneYet = setInterval(function checkIfDone() {
            if (runState.pendingScenarios === 0) {
                if (runState.pendingRequests !== 0) {
                    debug('DONE. Pending requests: %s', runState.pendingRequests);
                }

                clearInterval(doneYet);
                clearInterval(periodicStatsTimer);

                sendStats();

                intermediate.free();

                let aggregateReport = Stats.combine(aggregate).report();

                if (true) {
                    let logfile = getLogFilename(
                        '/tmp/latency-report.json',
                        ''
                    );
                    //if (false) {
                    console.log('Log file: %s', logfile);
                    //}
                    fs.writeFileSync(
                        logfile,
                        aggregateReport, {
                            flag: 'w'
                        }
                    );
                }

                let html_report = htmlreport();

                stsParams.RoleArn = 'arn:aws:iam::537237850156:role/' + crossAccountRole;

                Promise.resolve()
                    .then(() => { return setCreds(stsParams) })
                    .then(resourceParams => { return processEvent(resourceParams, html_report) })
                    .catch(error => console.error(error));

                return ee.emit('done', aggregateReport);
            } else {
                debug('Pending requests: %s', runState.pendingRequests);
                debug('Pending scenarios: %s', runState.pendingScenarios);
            }
        }, 500);
    });

    const periodicStatsTimer = setInterval(sendStats, options.periodicStats * 1000);

    function sendStats() {
        aggregate.push(intermediate.clone());
        intermediate._concurrency = runState.pendingScenarios;
        intermediate._pendingRequests = runState.pendingRequests;
        ee.emit('stats', intermediate.clone());
        intermediate.reset();
    }

    phaser.run();
}

function runScenario(script, intermediate, runState) {
    const start = process.hrtime();

    //
    // Compile scenarios if needed
    //
    if (!runState.compiledScenarios) {
        _.each(script.scenarios, function(scenario) {
            if (!scenario.weight) {
                scenario.weight = 1;
            }
        });

        runState.picker = wl(script.scenarios);

        runState.scenarioEvents = new EventEmitter();
        runState.scenarioEvents.on('customStat', function(stat) {
            intermediate.addCustomStat(stat.stat, stat.value);
        });
        runState.scenarioEvents.on('started', function() {
            runState.pendingScenarios++;
        });
        runState.scenarioEvents.on('error', function(errCode) {
            intermediate.addError(errCode);
        });
        runState.scenarioEvents.on('request', function() {
            intermediate.newRequest();

            runState.pendingRequests++;
        });
        runState.scenarioEvents.on('match', function() {
            intermediate.addMatch();
        });
        runState.scenarioEvents.on('response', function(delta, code, uid) {
            intermediate.completedRequest();
            intermediate.addLatency(delta);
            intermediate.addCode(code);

            let entry = [Date.now(), uid, delta, code];
            intermediate.addEntry(entry);

            runState.pendingRequests--;
        });

        runState.compiledScenarios = _.map(
            script.scenarios,
            function(scenarioSpec) {
                const name = scenarioSpec.engine || 'http';
                const engine = runState.engines.find((e) => e.__name === name);
                return engine.createScenario(scenarioSpec, runState.scenarioEvents);
            }
        );
    }

    let i = runState.picker()[0];

    debug('picking scenario %s (%s) weight = %s',
        i,
        script.scenarios[i].name,
        script.scenarios[i].weight);

    intermediate.newScenario(script.scenarios[i].name || i);

    const scenarioStartedAt = process.hrtime();
    const scenarioContext = createContext(script);
    const finish = process.hrtime(start);
    const runScenarioDelta = (finish[0] * 1e9) + finish[1];
    debugPerf('runScenarioDelta: %s', Math.round(runScenarioDelta / 1e6 * 100) / 100);
    runState.compiledScenarios[i](scenarioContext, function(err, context) {
        runState.pendingScenarios--;
        if (err) {
            debug(err);
        } else {
            const scenarioFinishedAt = process.hrtime(scenarioStartedAt);
            const delta = (scenarioFinishedAt[0] * 1e9) + scenarioFinishedAt[1];
            intermediate.addScenarioLatency(delta);
            intermediate.completedScenario();
        }
    });
}

/**
 * Create initial context for a scenario.
 */
function createContext(script) {
    const INITIAL_CONTEXT = {
        vars: {
            target: script.config.target,
            $environment: script._environment
        },
        funcs: {
            $randomNumber: $randomNumber,
            $randomString: $randomString
        }
    };
    let result = _.cloneDeep(INITIAL_CONTEXT);

    //
    // variables from payloads
    //
    if (script.config.payload) {
        _.each(script.config.payload, function(el) {
            let row = el.reader(el.data);
            _.each(el.fields, function(fieldName, j) {
                result.vars[fieldName] = row[j];
            });
        });
    }

    //
    // inline variables
    //
    if (script.config.variables) {
        _.each(script.config.variables, function(v, k) {
            let val;
            if (_.isArray(v)) {
                val = _.sample(v);
            } else {
                val = v;
            }
            result.vars[k] = val;
        });
    }
    result._uid = uuid.v4();
    return result;
}

//
// Generator functions for template strings:
//
function $randomNumber(min, max) {
    return _.random(min, max);
}

function $randomString(length) {
    return Math.random().toString(36).substr(2, length);
}


function htmlreport() {
    const defaultFormat = '[artillery_report_]YMMDD_HHmmSS[.html]';
    let dt = moment().format(defaultFormat);
    let reportFilename = '/tmp/' + dt; //artillery-report.html';

    let data = JSON.parse(fs.readFileSync('/tmp/latency-report.json', 'utf-8'));
    let templateFn = path.join(
        path.dirname(__filename),
        'index.html.ejs');
    let template = fs.readFileSync(templateFn, 'utf-8');
    let compiledTemplate = l.template(template);
    let html = compiledTemplate({ report: JSON.stringify(data, null, 2) });
    fs.writeFileSync(
        reportFilename,
        html, { encoding: 'utf-8', flag: 'w' });
    console.log('Report generated: %s', reportFilename);



    return dt;

}



function getLogFilename(output, userDefaultFilenameFormat) {
    let logfile;

    // is the destination a directory that exists?
    let isDir = false;
    if (output) {
        try {
            isDir = fs.statSync(output).isDirectory();
        } catch (err) {
            // ENOENT, don't need to do anything
        }
    }

    const defaultFormat = '[artillery_report_]YMMDD_HHmmSS[.json]';
    if (!isDir && output) {
        // -o is set with a filename (existing or not)
        logfile = output;
    } else if (!isDir && !output) {
        // no -o set
    } else {
        // -o is set with a directory
        logfile = path.join(
            output,
            moment().format(userDefaultFilenameFormat || defaultFormat)
        );
    }

    return logfile;
}