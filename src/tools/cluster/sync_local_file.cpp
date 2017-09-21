
#include "sync_file_base.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;

//#include <stdio.h>
//#include <stdlib.h>
//#include <tbsys.h>
//#include <TbThread.h>
//#include <vector>
//#include <string>
//#include <sys/types.h>
//#include <dirent.h>
//#include <libgen.h>
//#include <Mutex.h>
//#include <Memory.hpp>
//
//#include "common/func.h"
//#include "common/directory_op.h"
//#include "new_client/fsname.h"
//#include "new_client/tfs_client_impl.h"
//
//
//enum ActionInfo
//{
//  WRITE_ACTION = 1,
//  HIDE_SOURCE = 2,
//  DELETE_SOURCE = 4,
//  UNHIDE_SOURCE = 8,
//  UNDELE_SOURCE = 16,
//  HIDE_DEST = 32,
//  DELETE_DEST = 64,
//  UNHIDE_DEST = 128,
//  UNDELE_DEST = 256
//};
//enum StatFlag
//{
//  DELETE_FLAG = 1,
//  HIDE_FLAG = 4
//};

typedef vector<string> VEC_FILE_NAME;
typedef vector<string>::iterator VEC_FILE_NAME_ITER;
struct SyncStat
{
  int64_t total_count_;
  int64_t actual_count_;
  int64_t success_count_;
  int64_t fail_count_;
  tbutil::Mutex mutex_;
};
struct LogFile
{
  FILE** fp_;
  const char* file_;
};

int get_file_list(const string& log_file, VEC_FILE_NAME& name_set);
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, VEC_FILE_NAME& name_set, const bool force, const int32_t modify_time);
void trim(const char* input, char* output);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& local_dir, const string& dest_ns_addr, const bool force, const int32_t modify_time):
      local_dir_(local_dir), dest_ns_addr_(dest_ns_addr), force_(force), modify_time_(modify_time), destroy_(false)
    {
      TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, true);
    }
    virtual ~WorkThread()
    {

    }

    void wait_for_shut_down()
    {
      join();
    }

    void destroy()
    {
      destroy_ = true;
    }

    virtual void run()
    {
      if (! destroy_)
      {
        sync_file(local_dir_, dest_ns_addr_, name_set_, force_, modify_time_);
      }
    }

    void push_back(string& file_name)
    {
      name_set_.push_back(file_name);
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    string local_dir_;
    string dest_ns_addr_;
    VEC_FILE_NAME name_set_;
    bool force_;
    int32_t modify_time_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

SyncStat g_sync_stat_;
FILE *g_sync_succ = NULL;
FILE *g_sync_fail = NULL;
static const int WRITE_DATA_TMPBUF_SIZE = 2 * 1024 * 1024;
static WorkThreadPtr* gworks = NULL;
static int32_t thread_count = 1;
static const int32_t MAX_READ_LEN = 256;
//string src_local_file_ = "";
//string dest_ns_addr_ = "";

struct LogFile g_log_fp[] =
{
  {&g_sync_succ, "sync_succ_file"},
  {&g_sync_fail, "sync_fail_file"},
  {NULL, NULL}
};

int init_log_file(char* dir_path)
{
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s%s", dir_path, g_log_fp[i].file_);
    *g_log_fp[i].fp_ = fopen(file_path, "w");
    if (!*g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -d -f [-g] [-l] [-h]\n", name);
  fprintf(stderr, "       -s source dir\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f file list, assign a path to sync\n");
  fprintf(stderr, "       -m modify time, the file modified after it will be ignored\n");
  fprintf(stderr, "       -g log file name, redirect log info to log file, optional\n");
  fprintf(stderr, "       -l log file level, set log file level, optional\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

static void interrupt_callback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal);
  switch( signal )
  {
    case SIGTERM:
    case SIGINT:
    default:
      if (gworks != NULL)
      {
        for (int32_t i = 0; i < thread_count; ++i)
        {
          if (gworks != 0)
          {
            gworks[i]->destroy();
          }
        }
      }
      break;
  }
}

void trim(const char* input, char* output)
{
  if (output == NULL)
  {
    fprintf(stderr, "output str is null");
  }
  else
  {
    int32_t size = strlen(input);
    int32_t j = 0;
    for (int32_t i = 0; i < size; i++)
    {
      if (!isspace(input[i]))
      {
        output[j] = input[i];
        j++;
      }
    }
    output[j] = '\0';
  }
}

int get_file_list(const string& log_file, VEC_FILE_NAME& name_set)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen (log_file.c_str(), "r");
  if (fp == NULL)
  {
    TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", log_file.c_str(), strerror(errno));
  }
  else
  {
    char name[MAX_READ_LEN];
    char buf[MAX_READ_LEN];
    while(!feof(fp))
    {
      memset(buf, 0, MAX_READ_LEN);
      fgets (buf, MAX_READ_LEN, fp);
      trim(buf, name);
      if (strlen(name) > 0)
      {
        name_set.push_back(string(name));
        TBSYS_LOG(INFO, "name: %s\n", name);
      }
    }
    fclose (fp);
    ret = TFS_SUCCESS;
  }
  return ret;
}

int main(int argc, char* argv[])
{
  int32_t i;
  string src_local_dir = "", dest_ns_addr = "";
  string file_list= "";
  string log_file = "sync_report.log";
  string level = "info";
  string modify_time = "";
  bool force = false;

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:m:t:g:l:eh")) != EOF)
  {
    switch (i)
    {
      case 's':
        src_local_dir = optarg;
        break;
      case 'd':
        dest_ns_addr = optarg;
        break;
      case 'f':
        file_list = optarg;
        break;
      case 'e':
        force = true;
        break;
      case 'm':
        modify_time = optarg;
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'g':
        log_file = optarg;
        break;
      case 'l':
        level = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if ((src_local_dir.empty())
      || dest_ns_addr.empty()
      || (dest_ns_addr.compare(" ") == 0)
      || file_list.empty()
      || modify_time.empty())
  {
    usage(argv[0]);
  }

  modify_time += "000000";

  if ((level != "info") && (level != "debug") && (level != "error") && (level != "warn"))
  {
    fprintf(stderr, "level(info | debug | error | warn) set error\n");
    return TFS_ERROR;
  }

  char base_path[256];
  char log_path[256];
  snprintf(base_path, 256, "%s%s", dirname(argv[0]), "/log/");
  DirectoryOp::create_directory(base_path);
  init_log_file(base_path);

  snprintf(log_path, 256, "%s%s", base_path, log_file.c_str());
  if (strlen(log_path) != 0 && access(log_path, R_OK) == 0)
  {
    char old_log_file[256];
    sprintf(old_log_file, "%s.%s", log_path, Func::time_to_str(time(NULL), 1).c_str());
    rename(log_path, old_log_file);
  }
  else if (!DirectoryOp::create_full_path(log_path, true))
  {
    TBSYS_LOG(ERROR, "create file(%s)'s directory failed", log_path);
    return TFS_ERROR;
  }

  TBSYS_LOGGER.setFileName(log_path, true);
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);
  TBSYS_LOGGER.setLogLevel(level.c_str());

  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));

  VEC_FILE_NAME name_set;
  if (!file_list.empty())
  {
    get_file_list(file_list, name_set);
  }
  if (name_set.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    int32_t time = tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str()));
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_local_dir, dest_ns_addr, force, time);
    }
    int32_t index = 0;
    int64_t count = 0;
    VEC_FILE_NAME_ITER iter = name_set.begin();
    for (; iter != name_set.end(); iter++)
    {
      index = count % thread_count;
      gworks[index]->push_back(*iter);
      ++count;
    }
    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->start();
    }

    signal(SIGHUP, interrupt_callback);
    signal(SIGINT, interrupt_callback);
    signal(SIGTERM, interrupt_callback);
    signal(SIGUSR1, interrupt_callback);

    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->wait_for_shut_down();
    }

    tbsys::gDeleteA(gworks);
  }

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    fclose(*g_log_fp[i].fp_);
  }

  fprintf(stdout, "TOTAL COUNT: %"PRI64_PREFIX"d, ACTUAL_COUNT: %"PRI64_PREFIX"d, SUCCESS COUNT: %"PRI64_PREFIX"d, FAIL COUNT: %"PRI64_PREFIX"d\n",
      g_sync_stat_.total_count_, g_sync_stat_.actual_count_, g_sync_stat_.success_count_, g_sync_stat_.fail_count_);
  fprintf(stdout, "LOG FILE: %s\n", log_path);

  return TFS_SUCCESS;
}

int get_file_info(const string& tfs_client, const string& file_name, TfsFileStat& buf)
{
  int ret = TFS_SUCCESS;

  int fd = TfsClientImpl::Instance()->open(file_name.c_str(), NULL, tfs_client.c_str(), T_READ);
  if (fd < 0)
  {
    ret = TFS_ERROR;
    TBSYS_LOG(WARN, "open file(%s) failed, tfs_client: %s, ret: %d", file_name.c_str(), tfs_client.c_str(), ret);
  }
  else
  {
    ret = TfsClientImpl::Instance()->fstat(fd, &buf, FORCE_STAT);
    if (ret != TFS_SUCCESS)
    {
      TBSYS_LOG(WARN, "stat file(%s) failed, ret: %d", file_name.c_str(), ret);
    }
    TfsClientImpl::Instance()->close(fd);
  }
  return ret;
}

void change_flag(const int32_t dest_flag, SyncAction& sync_action)
{
  if (dest_flag & HIDE_FLAG)
  {
    sync_action.push_back(UNHIDE_DEST);
  }

  if (dest_flag & DELETE_FLAG)
  {
    sync_action.push_back(UNDELE_DEST);
  }
}

uint32_t file_crc(int fd, size_t file_size)
{
  uint32_t crc_value = 0;
  char data[WRITE_DATA_TMPBUF_SIZE];
  int len; 

  if (file_size > WRITE_DATA_TMPBUF_SIZE) {
    while ((len = read(fd, data, WRITE_DATA_TMPBUF_SIZE)) > 0) {
      crc_value = Func::crc(crc_value, data, len);
    }
  } else {
    len = read(fd, data, file_size);
    if (len <= 0) {
      return -1;
    }
    crc_value = Func::crc(0, data, len);
  }
  return crc_value;
}

int cmp_file_info(const string& file_name, const string& local_dir, const string& dest_ns_addr,
                  SyncAction& sync_action, const bool force, const int32_t modify_time)
{
  int ret = TFS_ERROR;
  int src_fd;
  
  struct stat src_file_stat;
  TfsFileStat dest_buf;

  memset(&src_file_stat, 0, sizeof(src_file_stat));
  string s = local_dir + "/" + file_name;
  src_fd = open(s.c_str(), O_RDONLY, 0);
  if (src_fd < 0)
  {
    return TFS_ERROR;
  }
  ret = fstat(src_fd, &src_file_stat);
  if (ret < 0)
  {
    return TFS_ERROR;
  }
  int64_t src_file_size = src_file_stat.st_size;
  uint32_t src_file_crc = file_crc(src_fd, src_file_stat.st_size);

  memset(&dest_buf, 0, sizeof(dest_buf));
  ret = get_file_info(dest_ns_addr, file_name, dest_buf);
  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(DEBUG, "get dest file info failed. filename: %s, ret: %d", file_name.c_str(), ret);
  }

  TBSYS_LOG(DEBUG, "file(%s): flag--(0 -> %d), crc--(%u -> %u), size--(%"PRI64_PREFIX"d -> %"PRI64_PREFIX"d)",
      file_name.c_str(), ((ret == TFS_SUCCESS)? dest_buf.flag_:-1), src_file_crc, dest_buf.crc_, src_file_size, dest_buf.size_);

  // 1. dest file exists and is new file, just skip.
  if (ret == TFS_SUCCESS)
  {
    if (dest_buf.modify_time_ > modify_time)
    {
      TBSYS_LOG(WARN, "dest file(%s) has been modifyed!!! %d->%d", file_name.c_str(), dest_buf.modify_time_, modify_time);
      return TFS_ERROR;
    }
    else if (dest_buf.size_ != src_file_size || dest_buf.crc_ != src_file_crc)
    {
      if (! force)
      {
        TBSYS_LOG(WARN, "file %s conflict!! src size: %lld crc: %llu -> dest size: %lld crc: %llu",
                  file_name.c_str(), src_file_size, src_file_crc, dest_buf.size_, dest_buf.crc_);
        return TFS_ERROR;
      }
      else
      {
        sync_action.push_back(WRITE_ACTION);
      }
    }
    else
    {
      change_flag(dest_buf.flag_, sync_action);
    }
  }
  else
  {
    sync_action.push_back(WRITE_ACTION);
  }
  return TFS_SUCCESS;
}

int copy_file(const string& file_name, const string& local_dir, const string& dest_ns_addr)
{
  int ret = TFS_SUCCESS;
  char data[MAX_READ_DATA_SIZE];
  int32_t rlen = 0;

  string full_path = local_dir + "/" + file_name;
  int source_fd = open(full_path.c_str(), O_RDONLY, 0);
  if (source_fd < 0)
  {
    TBSYS_LOG(ERROR, "open source local file fail when copy file, filename: %s", file_name.c_str());
    ret = TFS_ERROR;
  }
  int dest_fd = TfsClientImpl::Instance()->open(file_name.c_str(), NULL, dest_ns_addr.c_str(), T_WRITE | T_NEWBLK);
  if (dest_fd < 0)
  {
    TBSYS_LOG(ERROR, "open dest tfsfile fail when copy file, ret: %d, dest_ns: %s, filename: %s", dest_fd, dest_ns_addr.c_str(), file_name.c_str());
    ret = TFS_ERROR;
  }
  if (TFS_SUCCESS == ret)
  {
    if ((ret = TfsClientImpl::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "set option flag failed. ret: %d", ret);
    }
    else
    {
      for (;;)
      {
        rlen = read(source_fd, data, MAX_READ_DATA_SIZE);
        if (rlen < 0)
        {
          TBSYS_LOG(ERROR, "read tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
          ret = TFS_ERROR;
          break;
        }
        if (rlen == 0)
        {
          break;
        }

        if (TfsClientImpl::Instance()->write(dest_fd, data, rlen) != rlen)
        {
          TBSYS_LOG(ERROR, "write tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
          ret = TFS_ERROR;
          break;
        }

        if (rlen < MAX_READ_DATA_SIZE)
        {
          break;
        }
      }
    }
  }
  if (source_fd > 0)
  {
    close(source_fd);
  }
  if (dest_fd > 0)
  {
    if (TFS_SUCCESS != TfsClientImpl::Instance()->close(dest_fd))
    {
      ret = TFS_ERROR;
      TBSYS_LOG(ERROR, "close dest tfsfile fail, filename: %s", file_name.c_str());
    }
  }
  return ret;
}

int do_action(const string& file_name, const string& local_dir, const string& dest_ns_addr, const SyncAction& sync_action)
{
  int ret = TFS_SUCCESS;
  const ACTION_VEC& action_vec = sync_action.action_;
  ACTION_VEC_ITER iter = action_vec.begin();
  int32_t index = 0;

  for (; iter != action_vec.end(); iter++)
  {
    ActionInfo action = (*iter);
    int64_t file_size = 0;
    ret = TFS_SUCCESS;
    switch (action)
    {
      case WRITE_ACTION:
        ret = copy_file(file_name, local_dir, dest_ns_addr);
        break;
      case UNHIDE_DEST:
        ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_ns_addr.c_str(), REVEAL, TFS_FILE_NO_SYNC_LOG);
        usleep(20000);
        break;
      case UNDELE_DEST:
        ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_ns_addr.c_str(), UNDELETE, TFS_FILE_NO_SYNC_LOG);
        usleep(20000);
        break;
      default:
        break;
    }
    if (ret == TFS_SUCCESS)
    {
      TBSYS_LOG(DEBUG, "tfs file(%s) do (%d)th action(%d) success", file_name.c_str(), index, action);
      index++;
    }
    else
    {
      TBSYS_LOG(ERROR, "tfs file(%s) do (%d)th action(%d) failed, ret: %d", file_name.c_str(), index, action, ret);
      break;
    }
  }
  TBSYS_LOG(DEBUG, "do action finished. file_name: %s", file_name.c_str());
  return ret;
}

int sync_file(const string& local_dir, const string& dest_ns_addr, VEC_FILE_NAME& name_set, const bool force, const int32_t modify_time)
{
  int ret = TFS_SUCCESS;
  VEC_FILE_NAME_ITER iter = name_set.begin();
  for (; iter != name_set.end(); iter++)
  {
    string file_name = (*iter);
    SyncAction sync_action;

    ret = cmp_file_info(file_name, local_dir, dest_ns_addr, sync_action, force, modify_time);

    if (ret != TFS_ERROR)
    {
      ret = do_action(file_name, local_dir, dest_ns_addr, sync_action);
    }
    if (ret == TFS_SUCCESS)
    {
      TBSYS_LOG(INFO, "sync file(%s) succeed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_sync_stat_.mutex_);
        g_sync_stat_.success_count_++;
      }
      fprintf(g_sync_succ, "%s\n", file_name.c_str());
    }
    else
    {
      TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_sync_stat_.mutex_);
        g_sync_stat_.fail_count_++;
      }
      fprintf(g_sync_fail, "%s\n", file_name.c_str());
    }
    g_sync_stat_.actual_count_++;
  }
  return ret;
}

