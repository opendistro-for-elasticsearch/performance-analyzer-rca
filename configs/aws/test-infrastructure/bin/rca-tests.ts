#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { RcaTestsStack } from '../lib/rca-tests-stack';

const app = new cdk.App();
new RcaTestsStack(app, 'RcaTestsStack', {
    description: 'Resources for running RCA Integration Tests from a GitHub workflow',
});
