// <auto-generated />
#if !EXCLUDE_CODEGEN
#pragma warning disable 162
#pragma warning disable 219
#pragma warning disable 414
#pragma warning disable 618
#pragma warning disable 649
#pragma warning disable 693
#pragma warning disable 1591
#pragma warning disable 1998
[assembly: global::Orleans.Metadata.FeaturePopulatorAttribute(typeof (OrleansGeneratedCode.OrleansCodeGen0372aedef3FeaturePopulator))]
[assembly: global::System.CodeDom.Compiler.GeneratedCodeAttribute(@"Orleans-CodeGenerator", @"2.0.0.0")]
[assembly: global::Orleans.CodeGeneration.OrleansCodeGenerationTargetAttribute(@"GrainImplementation, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null")]
namespace OrleansGeneratedCode55A989F1
{
    using global::Orleans;
    using global::System.Reflection;
}

namespace OrleansGeneratedCode
{
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute(@"Orleans-CodeGenerator", @"2.0.0.0")]
    internal sealed class OrleansCodeGen0372aedef3FeaturePopulator : global::Orleans.Metadata.IFeaturePopulator<global::Orleans.Metadata.GrainInterfaceFeature>, global::Orleans.Metadata.IFeaturePopulator<global::Orleans.Metadata.GrainClassFeature>, global::Orleans.Metadata.IFeaturePopulator<global::Orleans.Serialization.SerializerFeature>
    {
        public void Populate(global::Orleans.Metadata.GrainInterfaceFeature feature)
        {
        }

        public void Populate(global::Orleans.Metadata.GrainClassFeature feature)
        {
            feature.Classes.Add(new global::Orleans.Metadata.GrainClassMetadata(typeof (global::SystemImplementation.CountWordStatefulOperator)));
            feature.Classes.Add(new global::Orleans.Metadata.GrainClassMetadata(typeof (global::SystemImplementation.CountWordStatelessOperator)));
            feature.Classes.Add(new global::Orleans.Metadata.GrainClassMetadata(typeof (global::SystemImplementation.TestStatelessOperator)));
            feature.Classes.Add(new global::Orleans.Metadata.GrainClassMetadata(typeof (global::SystemImplementation.TopologyManager)));
            feature.Classes.Add(new global::Orleans.Metadata.GrainClassMetadata(typeof (global::GrainImplementation.BatchCoodinator)));
            feature.Classes.Add(new global::Orleans.Metadata.GrainClassMetadata(typeof (global::GrainImplementation.BatchTracker)));
            feature.Classes.Add(new global::Orleans.Metadata.GrainClassMetadata(typeof (global::GrainImplementation.StreamSource)));
        }

        public void Populate(global::Orleans.Serialization.SerializerFeature feature)
        {
            feature.AddKnownType(@"SystemImplementation.CountWordStatefulOperator,GrainImplementation", @"SystemImplementation.CountWordStatefulOperator");
            feature.AddKnownType(@"GrainImplementation.StatefulStreamOperator,GrainImplementation", @"GrainImplementation.StatefulStreamOperator");
            feature.AddKnownType(@"SystemImplementation.CountWordStatelessOperator,GrainImplementation", @"SystemImplementation.CountWordStatelessOperator");
            feature.AddKnownType(@"SystemImplementation.StatelessStreamOperator,GrainImplementation", @"SystemImplementation.StatelessStreamOperator");
            feature.AddKnownType(@"SystemImplementation.PartitionFunction,GrainImplementation", @"SystemImplementation.PartitionFunction");
            feature.AddKnownType(@"SystemImplementation.TestStatelessOperator,GrainImplementation", @"SystemImplementation.TestStatelessOperator");
            feature.AddKnownType(@"SystemImplementation.TopologyManager,GrainImplementation", @"SystemImplementation.TopologyManager");
            feature.AddKnownType(@"GrainImplementation.BatchCoodinator,GrainImplementation", @"GrainImplementation.BatchCoodinator");
            feature.AddKnownType(@"GrainImplementation.BatchTracker,GrainImplementation", @"GrainImplementation.BatchTracker");
            feature.AddKnownType(@"GrainImplementation.StreamSource,GrainImplementation", @"GrainImplementation.StreamSource");
            feature.AddKnownType(@"Utils.Constants,Utils", @"Utils.Constants");
            feature.AddKnownType(@"Utils.Functions,Utils", @"Utils.Functions");
            feature.AddKnownType(@"Utils.PrettyConsole,Utils", @"Utils.PrettyConsole");
        }
    }
}
#pragma warning restore 162
#pragma warning restore 219
#pragma warning restore 414
#pragma warning restore 618
#pragma warning restore 649
#pragma warning restore 693
#pragma warning restore 1591
#pragma warning restore 1998
#endif
