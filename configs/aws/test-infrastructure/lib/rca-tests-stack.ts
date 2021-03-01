import * as cdk from '@aws-cdk/core';
import * as autoscaling from '@aws-cdk/aws-autoscaling';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as ecr from '@aws-cdk/aws-ecr';
import * as ecs from '@aws-cdk/aws-ecs';
import * as iam from '@aws-cdk/aws-iam';

export class RcaTestsStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const esPaRcaRepository = new ecr.Repository(this, 'RcaTestsPaRcaRepository', {
      repositoryName: 'rca-tests-es-pa-rca',
    });

    const testrunnerRepository = new ecr.Repository(this, 'RcaTestsTestrunnerRepository', {
      repositoryName: 'rca-tests-testrunner',
    });

    const vpc = new ec2.Vpc(this, 'RcaTestsVpc', {
      maxAzs: 3,
    });

    const ecsTaskSecurityGroup = new ec2.SecurityGroup(this, 'RcaTestsSecurityGroup', {
      vpc: vpc,
      allowAllOutbound: true,
    });

    const cluster = new ecs.Cluster(this, 'RcaTestsCluster', {
      clusterName: 'rca-tests-cluster',
      vpc: vpc,
    });

    const ecsCapacityUserData = ec2.UserData.forLinux();
    ecsCapacityUserData.addCommands(
        "sudo sysctl -w vm.max_map_count=262144"
    );

    const ecsCapacity = new autoscaling.AutoScalingGroup(this, 'RcaTestsEcsCapacity', {
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.LARGE),
      machineImage: ecs.EcsOptimizedImage.amazonLinux2(),
      vpc: vpc,
      allowAllOutbound: true,
      autoScalingGroupName: 'rca-tests-ecs-capacity',
      desiredCapacity: 1,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      userData: ecsCapacityUserData,
    });
    cluster.addAutoScalingGroup(ecsCapacity);

    const executionRole = new iam.Role(this, 'RcaTestsExecutionRole', {
      roleName: 'rca-tests-execution-role',
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });

    const executionPolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW
    });
    executionPolicy.addAllResources();
    executionPolicy.addActions(
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
    );
    executionRole.addToPolicy(executionPolicy);

    const githubEcsPolicyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
    });
    githubEcsPolicyStatement.addAllResources();
    githubEcsPolicyStatement.addActions(
        "ecr:GetAuthorizationToken",
        "ecs:DescribeTasks",
        "ecs:RegisterTaskDefinition",
        "ecs:RunTask",
    );

    const githubIamPolicyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
    });
    githubIamPolicyStatement.addResources(executionRole.roleArn);
    githubIamPolicyStatement.addActions("iam:PassRole");

    const githubPolicy = new iam.ManagedPolicy(this, 'RcaTestsGithubPolicy', {
      statements: [
          githubEcsPolicyStatement,
          githubIamPolicyStatement,
      ]
    });

    const githubUser = new iam.User(this, 'RcaTestsUser', {
      userName: 'rca-tests-github-user',
      managedPolicies: [
          githubPolicy,
      ]
    });

    const ecrRepositoryPolicyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
    });
    ecrRepositoryPolicyStatement.addArnPrincipal(githubUser.userArn);
    ecrRepositoryPolicyStatement.addArnPrincipal(executionRole.roleArn);
    ecrRepositoryPolicyStatement.addActions(
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:BatchCheckLayerAvailability",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
    );
    esPaRcaRepository.addToResourcePolicy(ecrRepositoryPolicyStatement);
    testrunnerRepository.addToResourcePolicy(ecrRepositoryPolicyStatement);

    const githubUserAccessKey = new iam.CfnAccessKey(githubUser, 'RcaTestsGithubAccessKey', {
      userName: githubUser.userName,
    });

    new cdk.CfnOutput(this, 'GithubUserAccessKeyId', { value: githubUserAccessKey.ref });
    new cdk.CfnOutput(this, 'GithubUserSecretAccessKey', { value: githubUserAccessKey.attrSecretAccessKey });
    new cdk.CfnOutput(this, 'RcaTestsSecurityGroupId', { value: ecsTaskSecurityGroup.securityGroupId });

    vpc.publicSubnets.forEach((subnet, i) => {
      new cdk.CfnOutput(this, `RcaTestsPublicSubnet${i}`, { value: subnet.subnetId });
    })
  }
}
