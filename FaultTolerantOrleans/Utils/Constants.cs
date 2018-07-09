using System.Collections.Generic;

namespace Utils
{
	public static class Constants
	{
        public const string FaultTolerantStreamProvider = "UU";
        public const string FaultTolerantStreamNameSpace = "YOLO";
        public const string ClusterId = "FT-deployment1";
        public const string ServiceId = "FTApp";
        public const string Barrier_Value = "Barrier";
        public const string System_Key = "System";
        public const string Commit_Value = "Commit";
        public const string Recovery_Value = "Recovery";
        public const string Tracker = "Tracker";
        public const string Coordinator = "Coordinator";
        public const string Topology_Manager = "Topology Manager";
        public const string Sentence_Generator = "Sentence Generator";
        public const string Stateless_Operator_Prefix = "SystemImplementation.CountWordStatelessOperator";
        public const string Stateful_Operator_Prefix = "SystemImplementation.CountWordStatefulOperator";
        public const int Checkpoint_Interval = 2;
    }
}