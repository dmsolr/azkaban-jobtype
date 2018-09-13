package cn.dmsolr.azkaban.hive;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import azkaban.jobExecutor.ProcessJob;
import azkaban.project.DirectoryFlowLoader;
import azkaban.server.AzkabanServer;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class DmHiveJob extends ProcessJob {
	public static String DMHiveWrapperClass = "com.watsons.dazkaban.DmHiveWrapper";
	public static final String CLASSPATH = "classpath";
	public static final String GLOBAL_CLASSPATH = "global.classpaths";
	public static final String JAVA_CLASS = "java.class";
	public static final String INITIAL_MEMORY_SIZE = "Xms";
	public static final String MAX_MEMORY_SIZE = "Xmx";
	public static final String MAIN_ARGS = "main.args";
	public static final String JVM_PARAMS = "jvm.args";
	public static final String GLOBAL_JVM_PARAMS = "global.jvm.args";

	public static final String DEFAULT_INITIAL_MEMORY_SIZE = "64M";
	public static final String DEFAULT_MAX_MEMORY_SIZE = "256M";

	public static String JAVA_COMMAND = "java";
	
	public DmHiveJob(String jobid, Props sysProps, Props jobProps, org.apache.log4j.Logger logger) {
		super(jobid, sysProps, jobProps, logger);
	}

	protected String getJavaClass() {
		return DMHiveWrapperClass;
	}

	@Override
	protected List<String> getCommandList() {
		final ArrayList<String> list = new ArrayList<>();
		list.add(createCommandLine());
		return list;
	}

	protected String createCommandLine() {
		String command = JAVA_COMMAND + " ";
		command += getJVMArguments() + " ";
		command += "-Xms" + getInitialMemorySize() + " ";
		command += "-Xmx" + getMaxMemorySize() + " ";
		command += "-cp " + createArguments(getClassPaths(), ":") + " ";
		command += getJavaClass() + " ";
		command += getMainArguments();

		return command;
	}

	protected List<String> getClassPaths() {
		final List<String> classPaths = getJobProps().getStringList(CLASSPATH, null, ",");

		final ArrayList<String> classpathList = new ArrayList<>();
		// Adding global properties used system wide.
		if (getJobProps().containsKey(GLOBAL_CLASSPATH)) {
			final List<String> globalClasspath = getJobProps().getStringList(GLOBAL_CLASSPATH);
			for (final String global : globalClasspath) {
				getLog().info("Adding to global classpath:" + global);
				classpathList.add(global);
			}
		}

		if (classPaths == null) {
			final File path = new File(getPath());
			// File parent = path.getParentFile();
			getLog().info("No classpath specified. Trying to load classes from " + path);

			if (path != null) {
				for (final File file : path.listFiles()) {
					if (file.getName().endsWith(".jar")) {
						// log.info("Adding to classpath:" + file.getName());
						classpathList.add(file.getName());
					}
				}
			}
		} else {
			classpathList.addAll(classPaths);
		}

		return classpathList;
	}

	protected String getInitialMemorySize() {
		return getJobProps().getString(INITIAL_MEMORY_SIZE, DEFAULT_INITIAL_MEMORY_SIZE);
	}

	protected String getMaxMemorySize() {
		return getJobProps().getString(MAX_MEMORY_SIZE, DEFAULT_MAX_MEMORY_SIZE);
	}

	protected String getMainArguments() {
		return getJobProps().getString(MAIN_ARGS, "");
	}

	protected String getJVMArguments() {
		final String globalJVMArgs = getJobProps().getString(GLOBAL_JVM_PARAMS, null);

		if (globalJVMArgs == null) {
			return getJobProps().getString(JVM_PARAMS, "");
		}

		return globalJVMArgs + " " + getJobProps().getString(JVM_PARAMS, "");
	}

	protected String createArguments(final List<String> arguments, final String separator) {
		if (arguments != null && arguments.size() > 0) {
			String param = "";
			for (final String arg : arguments) {
				param += arg + separator;
			}

			return param.substring(0, param.length() - 1);
		}

		return "";
	}

	@Override
	protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
		final String strXms = getInitialMemorySize();
		final String strXmx = getMaxMemorySize();
		final long xms = Utils.parseMemString(strXms);
		final long xmx = Utils.parseMemString(strXmx);

		final Props azkabanProperties = AzkabanServer.getAzkabanProperties();
		if (azkabanProperties != null) {
			final String maxXms = azkabanProperties.getString(DirectoryFlowLoader.JOB_MAX_XMS,
					DirectoryFlowLoader.MAX_XMS_DEFAULT);
			final String maxXmx = azkabanProperties.getString(DirectoryFlowLoader.JOB_MAX_XMX,
					DirectoryFlowLoader.MAX_XMX_DEFAULT);
			final long sizeMaxXms = Utils.parseMemString(maxXms);
			final long sizeMaxXmx = Utils.parseMemString(maxXmx);

			if (xms > sizeMaxXms) {
				throw new Exception(
						String.format("%s: Xms value has exceeded the allowed limit (max Xms = %s)", getId(), maxXms));
			}

			if (xmx > sizeMaxXmx) {
				throw new Exception(
						String.format("%s: Xmx value has exceeded the allowed limit (max Xmx = %s)", getId(), maxXmx));
			}
		}

		return new Pair<>(xms, xmx);
	}
}
