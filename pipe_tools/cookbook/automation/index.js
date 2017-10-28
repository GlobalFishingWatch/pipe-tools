/**
 * HTTP Cloud Function to run a dataflow template
 *
 * @param {Object} req Cloud Function request context.
 * @param {Object} res Cloud Function response context.

 based on https://dzone.com/articles/triggering-dataflow-pipelines-with-cloud-functions

 */

const google = require('googleapis');


exports.copyFiles = function copyFiles (req, res) {
    const projectId=req.body.projectId;
    const bucketName=req.body.bucketName;

    const dryRun = (typeof req.body.dryRun === 'undefined') ? false : req.body.dryRun;

    const inputFile = `gs://${bucketName}/pipe-tools-cookbook/automation/cloud-fn/sample-in.txt`;
    const outputFilePrefix = `gs://${bucketName}/pipe-tools-cookbook/automation/cloud-fn/sample-out`;
    const code = 'cloud-fn';
    const jobName = (typeof req.body.jobName === 'undefined') ? 'copyfile-cloud-fn' : req.body.jobName;;
    const jobTemplate = `gs://${bucketName}/pipe-tools-cookbook/automation/templates/copyfile`;

    if (typeof projectId === 'undefined' || typeof bucketName === 'undefined') {
        res.send ('Error:  projectId and bucketName are required.')
        return
    }

    res.send(
`

Triggering copyFile

dryRun: ${dryRun}
projectId: ${projectId}
template: ${jobTemplate}
jobName: ${jobName}
input: ${inputFile}
output: ${outputFilePrefix}
code: ${code}
`
    );

    if (dryRun) {
        return
    }
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
            projectId: projectId,
            resource: {
                parameters: {
                    input: inputFile,
                    output: outputFilePrefix,
                    code: code
                },
                jobName: jobName,
                gcsPath: jobTemplate
            }
        }, function(err, response) {
            if (err) {
                console.error("problem running dataflow template, error was: ", err);
            }
            console.log("Dataflow template response: ", response);
        });
    });
};
