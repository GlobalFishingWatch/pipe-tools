/**
 * HTTP Cloud Function to run a dataflow template
 *
 * @param {Object} req Cloud Function request context.
 * @param {Object} res Cloud Function response context.

 based on https://dzone.com/articles/triggering-dataflow-pipelines-with-cloud-functions

 */

const google = require('googleapis');

exports.copyFiles = function copyFiles (req, res) {
    res.send('Triggering copyFile.');

    google.auth.getApplicationDefault(function(err, authClient) {
        if (err) {
            throw err;
        }
        if (authClient.createScopedRequired && authClient.createScopedRequired()) {
            authClient = authClient.createScoped([
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/userinfo.email'
            ]);
        }
        const dataflow = google.dataflow({
            version: 'v1b3',
            auth: authClient
        });
        dataflow.projects.templates.create({
            projectId: 'world-fishing-827',
            resource: {
                parameters: {
                    input: 'gs://scratch-automation/pipe-tools-cookbook/automation/cloud-fn/sample-in.txt',
                    output: 'gs://scratch-automation/pipe-tools-cookbook/automation/cloud-fn/sample-out',
                    code: 'cloud-fn'
                },
                jobName: 'copyfile-cloud_fn',
                gcsPath: 'gs://scratch-automation/pipe-tools-cookbook/automation/templates/copyfile'
            }
        }, function(err, response) {
            if (err) {
                console.error("problem running dataflow template, error was: ", err);
            }
            console.log("Dataflow template response: ", response);
        });
    });
};
