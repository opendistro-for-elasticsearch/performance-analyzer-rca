package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.HardwareEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.ResourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResourceTypeUtilTest {
    private static ResourceType jvmOldGen;
    private static ResourceType jvmYoungGen;
    private static ResourceType hardwareCpu;
    private static ResourceType unrecognized;

    @BeforeClass
    public static void setup() {
        jvmOldGen = ResourceType.newBuilder().setJVM(JvmEnum.OLD_GEN).build();
        jvmYoungGen = ResourceType.newBuilder().setJVM(JvmEnum.YOUNG_GEN).build();
        hardwareCpu = ResourceType.newBuilder().setHardwareResourceType(HardwareEnum.CPU).build();
        unrecognized = ResourceType.newBuilder().build();
    }

    @Test
    public void testGetResourceTypeName() {
        Assert.assertEquals(ResourceTypeUtil.UNKNOWN_RESOURCE_TYPE_NAME,
                ResourceTypeUtil.getResourceTypeName(unrecognized));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeOptions(JvmEnum.OLD_GEN).getResourceTypeName(),
                ResourceTypeUtil.getResourceTypeName(jvmOldGen));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeOptions(JvmEnum.YOUNG_GEN).getResourceTypeName(),
                ResourceTypeUtil.getResourceTypeName(jvmYoungGen));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeOptions(HardwareEnum.CPU).getResourceTypeName(),
                ResourceTypeUtil.getResourceTypeName(hardwareCpu));
    }

    @Test
    public void testGetResourceTypeUnit() {
        Assert.assertEquals(ResourceTypeUtil.UNKNOWN_RESOURCE_TYPE_UNIT,
                ResourceTypeUtil.getResourceTypeUnit(unrecognized));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeOptions(JvmEnum.OLD_GEN).getResourceTypeUnit(),
                ResourceTypeUtil.getResourceTypeUnit(jvmOldGen));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeOptions(JvmEnum.YOUNG_GEN).getResourceTypeUnit(),
                ResourceTypeUtil.getResourceTypeUnit(jvmYoungGen));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeOptions(HardwareEnum.CPU).getResourceTypeUnit(),
                ResourceTypeUtil.getResourceTypeUnit(hardwareCpu));
    }

    @Test
    public void testBuildResourceType() {
        Assert.assertNull(ResourceTypeUtil.buildResourceType(null));
        Assert.assertNull(ResourceTypeUtil.buildResourceType("unrecognized"));
        ResourceType oldGen = ResourceTypeUtil.buildResourceType(
                ResourceTypeUtil.getResourceTypeOptions(JvmEnum.OLD_GEN).getResourceTypeName());
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeName(jvmOldGen),
                ResourceTypeUtil.getResourceTypeName(oldGen));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeUnit(jvmOldGen),
                ResourceTypeUtil.getResourceTypeUnit(oldGen));
        ResourceType youngGen = ResourceTypeUtil.buildResourceType(
                ResourceTypeUtil.getResourceTypeOptions(JvmEnum.YOUNG_GEN).getResourceTypeName());
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeName(jvmYoungGen),
                ResourceTypeUtil.getResourceTypeName(youngGen));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeUnit(jvmYoungGen),
                ResourceTypeUtil.getResourceTypeUnit(youngGen));
        ResourceType cpu = ResourceTypeUtil.buildResourceType(
                ResourceTypeUtil.getResourceTypeOptions(HardwareEnum.CPU).getResourceTypeName());
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeName(hardwareCpu),
                ResourceTypeUtil.getResourceTypeName(cpu));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeUnit(hardwareCpu),
                ResourceTypeUtil.getResourceTypeUnit(cpu));
    }
}
