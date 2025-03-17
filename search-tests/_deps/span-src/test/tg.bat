@echo off & setlocal enableextensions enabledelayedexpansion
::
:: tg.bat - compile & run tests (GNUC).
::

set      unit=span
set unit_file=span

:: if no std is given, use c++11

:: if no std is given, use c++11

set std=c++11
if NOT "%1" == "" set std=%1 & shift

set UCAP=%unit%
call :toupper UCAP

set unit_select=%unit%_%UCAP%_DEFAULT
::set unit_select==%unit%_%UCAP%_NONSTD
::set unit_select=%unit%_%UCAP%_STD
if NOT "%1" == "" set unit_select=%1 & shift

set args=%1 %2 %3 %4 %5 %6 %7 %8 %9

set gpp=g++

call :CompilerVersion version
echo %gpp% %version%: %std% %unit_select% %args%

set unit_contract=^
    -Dspan_CONFIG_SELECT_SPAN=%unit_select% ^
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
set warnflags=-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wno-padded -Wno-missing-noreturn

%gpp% -std=%std% %optflags% %warnflags% %unit_contract% %unit_config% %byte_lite% -o %unit%-main.t.exe -isystem lest -I../include -I. %unit%-main.t.cpp %unit%.t.cpp && %unit%-main.t.exe

endlocal & goto :EOF

:: subroutines:

:CompilerVersion  version
echo off & setlocal enableextensions
set tmpprogram=_getcompilerversion.tmp
set tmpsource=%tmpprogram%.c

echo #include ^<stdio.h^>     > %tmpsource%
echo int main(){printf("%%d.%%d.%%d\n",__GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__);} >> %tmpsource%

%gpp% -o %tmpprogram% %tmpsource% >nul
for /f %%x in ('%tmpprogram%') do set version=%%x
del %tmpprogram%.* >nul
endlocal & set %1=%version%& goto :EOF

:: toupper; makes use of the fact that string
:: replacement (via SET) is not case sensitive
:toupper
for %%L IN (A B C D E F G H I J K L M N O P Q R S T U V W X Y Z) DO SET %1=!%1:%%L=%%L!
goto :EOF

