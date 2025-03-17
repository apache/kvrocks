@echo off & setlocal enableextensions enabledelayedexpansion
::
:: tc.bat - compile & run tests (clang).
::

set unit=span

:: if no std is given, use c++14

set std=%1
if "%std%"=="" set std=c++14

set  clang=clang

call :CompilerVersion version
echo %clang% %version%: %std%

set UCAP=%unit%
call :toupper UCAP

set unit_select=-D%unit%_CONFIG_SELECT_%UCAP%=%unit%_%UCAP%_DEFAULT
::set unit_select=-D%unit%_CONFIG_SELECT_%UCAP%=%unit%_%UCAP%_NONSTD
::set unit_select=-D%unit%_CONFIG_SELECT_%UCAP%=%unit%_%UCAP%_STD

set unit_contract=^
    -Dspan_CONFIG_CONTRACT_VIOLATION_TERMINATES=0 ^
    -Dspan_CONFIG_CONTRACT_VIOLATION_THROWS=1

:: Alternative flags:
:: -Dspan_FEATURE_WITH_CONTAINER=1 takes precedence over span_FEATURE_WITH_CONTAINER_TO_STD
:: -Dspan_FEATURE_MAKE_SPAN=1      takes precedence over span_FEATURE_MAKE_SPAN_TO_STD

set unit_config=^
    -Dspan_FEATURE_CONSTRUCTION_FROM_STDARRAY_ELEMENT_TYPE=1 ^
    -Dspan_FEATURE_WITH_INITIALIZER_LIST_P2447=1 ^
    -Dspan_FEATURE_WITH_CONTAINER_TO_STD=99 ^
    -Dspan_FEATURE_MEMBER_CALL_OPERATOR=1 ^
    -Dspan_FEATURE_MEMBER_AT=2 ^
    -Dspan_FEATURE_MEMBER_BACK_FRONT=1 ^
    -Dspan_FEATURE_MEMBER_SWAP=1 ^
    -Dspan_FEATURE_COMPARISON=1 ^
    -Dspan_FEATURE_SAME=1 ^
    -Dspan_FEATURE_NON_MEMBER_FIRST_LAST_SUB=0 ^
    -Dspan_FEATURE_NON_MEMBER_FIRST_LAST_SUB_SPAN=1 ^
    -Dspan_FEATURE_NON_MEMBER_FIRST_LAST_SUB_CONTAINER=1 ^
    -Dspan_FEATURE_MAKE_SPAN_TO_STD=99 ^
    -Dspan_FEATURE_BYTE_SPAN=1

set byte_lite=^
    -Dspan_BYTE_LITE_HEADER=\"../../byte-lite/include/nonstd/byte.hpp\"

rem -flto / -fwhole-program
set  optflags=-O2
set warnflags=-Wall -Wextra -Wpedantic -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-padded -Wno-missing-noreturn -Wno-documentation-unknown-command -Wno-documentation-deprecated-sync -Wno-documentation -Wno-weak-vtables -Wno-missing-prototypes -Wno-missing-variable-declarations -Wno-exit-time-destructors -Wno-global-constructors

"%clang%" -m32 -std=%std% %optflags% %warnflags% %unit_select% %unit_contract% %unit_config% %byte_lite% -fms-compatibility-version=19.00 -isystem "%VCInstallDir%include" -isystem "%WindowsSdkDir_71A%include" -isystem lest -I../include -I. -o %unit%-main.t.exe %unit%-main.t.cpp %unit%.t.cpp && %unit%-main.t.exe
endlocal & goto :EOF

:: subroutines:

:CompilerVersion  version
echo off & setlocal enableextensions
set tmpprogram=_getcompilerversion.tmp
set tmpsource=%tmpprogram%.c

echo #include ^<stdio.h^>     > %tmpsource%
echo int main(){printf("%%d.%%d.%%d\n",__clang_major__,__clang_minor__,__clang_patchlevel__);} >> %tmpsource%

"%clang%" -m32 -o %tmpprogram% %tmpsource% >nul
for /f %%x in ('%tmpprogram%') do set version=%%x
del %tmpprogram%.* >nul
endlocal & set %1=%version%& goto :EOF

:: toupper; makes use of the fact that string
:: replacement (via SET) is not case sensitive
:toupper
for %%L IN (A B C D E F G H I J K L M N O P Q R S T U V W X Y Z) DO SET %1=!%1:%%L=%%L!
goto :EOF
