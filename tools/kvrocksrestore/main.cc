#include <getopt.h>
#include <stdlib.h>
#include <glog/logging.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/backupable_db.h"
#ifdef __linux__
#define _XOPEN_SOURCE 700
#else
#define _XOPEN_SOURCE
#endif
#include <signal.h>
#include <execinfo.h>
#include <ucontext.h>
#include <iostream>

#include "version.h"

#if defined(__APPLE__) || defined(__linux__)
#define HAVE_BACKTRACE 1
#endif

const char *kDefaultBackupDir = "./backup";
const char *kDefaultDbDir = "./db";

std::function<void()> hup_handler;

struct Options {
  std::string backup_dir = kDefaultBackupDir;
  std::string db_dir = kDefaultDbDir;
  bool show_usage = false;
};

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

#ifdef HAVE_BACKTRACE
void *getMcontextEip(ucontext_t *uc) {
#ifdef __x86_64__
#define REG_EIP REG_RIP
#endif
#if defined(__FreeBSD__)
  return reinterpret_cast<void*>(uc->uc_mcontext.mc_eip);
#elif defined(__dietlibc__)
  return reinterpret_cast<void*>(uc->uc_mcontext.eip);
#elif defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
#if __x86_64__
  return reinterpret_cast<void *>(uc->uc_mcontext->__ss.__rip);
#else
  return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__eip);
#endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
#if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
  return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__rip);
#else
  return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__eip);
#endif
#elif defined(__i386__) || defined(__X86_64__) || defined(__x86_64__)
  return reinterpret_cast<void*>(uc->uc_mcontext.gregs[REG_EIP]); /* Linux 32/64 bit */
#elif defined(__ia64__) /* Linux IA64 */
  return reinterpret_cast<void*>(uc->uc_mcontext.sc_ip);
#endif
  return nullptr;
}

extern "C" void segvHandler(int sig, siginfo_t *info, void *secret) {
  void *trace[100];
  char **messages = nullptr;
  struct sigaction act;
  auto uc = reinterpret_cast<ucontext_t *>(secret);

  LOG(WARNING) << "======= Ooops! kvrocksrestore " << VERSION << " got signal: " << sig << " =======";
  int trace_size = backtrace(trace, 100);
  /* overwrite sigaction with caller's address */
  if (getMcontextEip(uc) != nullptr) {
    trace[1] = getMcontextEip(uc);
  }
  messages = backtrace_symbols(trace, trace_size);
  for (int i = 1; i < trace_size; ++i) {
    LOG(WARNING) << messages[i];
  }
  /* Make sure we exit with the right signal at the end. So for instance
   * the core will be dumped if enabled.
   */
  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used
   */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, nullptr);
  kill(getpid(), sig);
}

void setupSigSegvAction() {
  struct sigaction act;

  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
  act.sa_sigaction = segvHandler;
  sigaction(SIGSEGV, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);
  sigaction(SIGFPE, &act, nullptr);
  sigaction(SIGILL, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);

  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = signal_handler;
  sigaction(SIGTERM, &act, nullptr);
  sigaction(SIGINT, &act, nullptr);
}

#else /* HAVE_BACKTRACE */
void setupSigSegvAction() {
}
#endif /* HAVE_BACKTRACE */

static void usage(const char *program) {
  std::cout << program << " restore kvrocks from backup_dir \n"
            << "\t-b backup_dir, default is " << kDefaultBackupDir << "\n"
            << "\t-d db_dir, default is " << kDefaultDbDir << "\n"
            << "\t-h help\n";
  exit(0);
}

static Options parseCommandLineOptions(int argc, char **argv) {
  int ch;
  Options opts;
  while ((ch = ::getopt(argc, argv, "b:d:hv")) != -1) {
    switch (ch) {
      case 'b':opts.backup_dir = optarg;
        break;
      case 'd':opts.db_dir = optarg;
        break;
      case 'h': opts.show_usage = true;
        break;
      case 'v': exit(0);
      default: usage(argv[0]);
    }
  }
  return opts;
}

static void initGoogleLog(int loglevel = 0, const std::string &log_dir = "./") {
  FLAGS_minloglevel = loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = log_dir;
}

int main(int argc, char *argv[]) {
  google::InitGoogleLogging("kvrocksrestore");
  initGoogleLog();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  setupSigSegvAction();

  std::cout << "Version: " << VERSION << " @" << GIT_COMMIT << std::endl;
  auto opts = parseCommandLineOptions(argc, argv);
  if (opts.show_usage) usage(argv[0]);

  rocksdb::BackupEngineReadOnly *backup_engine;
  auto s = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(),
                                               rocksdb::BackupableDBOptions(opts.backup_dir),
                                               &backup_engine);
  if (!s.ok()) {
    std::cout << "backup_dir is not valid or doesn't exist: " << opts.backup_dir << std::endl;
    exit(1);
  }
  LOG(INFO) << "Restore started... " << std::endl;
  s = backup_engine->RestoreDBFromLatestBackup(opts.db_dir, opts.db_dir);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to restore: " << s.ToString() << std::endl;
    exit(1);
  }
  LOG(INFO) << "Success restored!" << std::endl;
  delete backup_engine;

  hup_handler = [&backup_engine] {
    LOG(INFO) << "Bye Bye";
  };
  return 0;
}